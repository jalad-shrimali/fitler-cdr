package vi
// filter vi CDRs
import (
	"embed"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

/* ── canonical 26-column output header (keep order) ─────────────────────── */
var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address",
	"Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Main City(First CellID)", "Sub City (First CellID)",
	"Lat-Long-Azimuth (First CellID)",
	"Crime", "Circle", "Operator", "LRN",
	"CallForward", "B Party Provider", "B Party Circle",
	"B Party Operator", "Type", "IMEI Manufacturer",
}

/* ── tiny helpers ───────────────────────────────────────────────────────── */
var (
	spaceRE  = regexp.MustCompile(`\s+`)
	nonDigit = regexp.MustCompile(`\D`)
)

func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }

func colIdxAny(header []string, keys ...string) int {
	for _, k := range keys {
		if idx := colIdx(header, k); idx != -1 {
			return idx
		}
	}
	return -1
}

/* ── banner-style CDR extractor ─────────────────────────────────────────── */
/* matches: MSISDN 123, MSISDN:123, MSISDN - 123, MSISDN:-123 … */
var msisdnRE = regexp.MustCompile(`(?i)msisdn[^0-9]*([0-9]{8,15})`)

func extractCdrNumber(line string) string {
	if m := msisdnRE.FindStringSubmatch(line); len(m) > 1 {
		return m[1]
	}
	return ""
}

/* ── embedded reference data ────────────────────────────────────────────── */
// go:embed data/*
var dataFS embed.FS

/* ---------- VI cell DB ---------- */
type CellInfo struct{ Addr, Sub, Main, LatLonAz string }

var cellDB = map[string]map[string]CellInfo{}

/* ---------- LRN DB (shared) ---------- */
type LRNInfo struct{ Provider, Circle, Operator string }

var lrnDB = map[string]LRNInfo{}

/* ── init: load reference CSVs ──────────────────────────────────────────── */
func init() {
	if err := loadCells("vi", "data/vi_cells.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("load vi_cells.csv: %v", err)
	}
	if err := loadLRN("data/LRN.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("warning: LRN.csv missing – provider/circle/operator columns will stay blank (%v)", err)
	}
}

/* ── load cell database ─────────────────────────────────────────────────── */
func loadCells(tsp, path string) error {
	f, err := dataFS.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, _ := r.Read()

	col := func(keys ...string) int {
		for i, h := range header {
			for _, k := range keys {
				if norm(h) == k {
					return i
				}
			}
		}
		return -1
	}
	iCGI := col("cgi", "cell global id", "cell id")
	iAddr := col("address", "bts location")
	iSub := col("subcity", "sub city")
	iMain := col("maincity", "city", "main city")
	iLat := col("latitude", "lat")
	iLon := col("longitude", "lon", "long")
	iAz := col("azimuth", "azm", "az")

	if iCGI == -1 {
		return fmt.Errorf("no CGI column in %s", path)
	}
	cellDB[tsp] = map[string]CellInfo{}
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) == 0 {
			continue
		}
		cgi := strings.TrimSpace(rec[iCGI])
		if cgi == "" {
			continue
		}
		info := CellInfo{
			Addr:     pick(rec, iAddr),
			Sub:      pick(rec, iSub),
			Main:     pick(rec, iMain),
			LatLonAz: buildLat(rec, iLat, iLon, iAz),
		}
		cellDB[tsp][cgi] = info
		cellDB[tsp][nonDigit.ReplaceAllString(cgi, "")] = info // digits-only key
	}
	return nil
}

/* ── load LRN reference ─────────────────────────────────────────────────── */
func loadLRN(path string) error {
	f, err := dataFS.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, _ := r.Read()

	iLRN := colIdxAny(header, "lrn no", "lrn", "lrn number")
	iTSP := colIdxAny(header, "tsp", "provider", "tsp-lsa")
	iCircle := colIdxAny(header, "circle")
	if iLRN == -1 {
		return fmt.Errorf("no LRN column in %s", path)
	}
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) == 0 {
			continue
		}
		lrn := nonDigit.ReplaceAllString(rec[iLRN], "")
		if lrn == "" {
			continue
		}
		lrnDB[lrn] = LRNInfo{
			Provider: pick(rec, iTSP),
			Circle:   pick(rec, iCircle),
			Operator: pick(rec, iTSP), // Operator = tsp (spec)
		}
	}
	return nil
}

/* ── small utilities ───────────────────────────────────────────────────── */
func pick(rec []string, idx int) string {
	if idx == -1 || idx >= len(rec) {
		return ""
	}
	return strings.TrimSpace(rec[idx])
}
func buildLat(rec []string, iLat, iLon, iAz int) string {
	if iLat == -1 || iLon == -1 {
		return ""
	}
	lat, lon := pick(rec, iLat), pick(rec, iLon)
	if lat == "" || lon == "" {
		return ""
	}
	if az := pick(rec, iAz); az != "" {
		return lat + ", " + lon + ", " + az
	}
	return lat + ", " + lon
}
func findCell(tsp, id string) (CellInfo, bool) {
	db := cellDB[tsp]
	if info, ok := db[id]; ok {
		return info, true
	}
	// try digits-only key
	if info, ok := db[nonDigit.ReplaceAllString(id, "")]; ok {
		return info, true
	}
	return CellInfo{}, false
}

/* ── HTTP handler ───────────────────────────────────────────────────────── */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	if strings.ToLower(r.FormValue("tsp_type")) != "vi" {
		http.Error(w, "Only VI supported here", 400)
		return
	}
	crime := r.FormValue("crime_number")

	file, hdr, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer file.Close()

	_ = os.MkdirAll("uploads", 0o755)
	_ = os.MkdirAll("filtered", 0o755)

	src := filepath.Join("uploads", hdr.Filename)
	if err := saveUploaded(file, src); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	out, err := normVI(src, crime)
	if err != nil {
		http.Error(w, "normalisation failed: "+err.Error(), 500)
		return
	}
	fmt.Fprintf(w, "/download/%s\n", filepath.Base(out))
}

/* save uploaded file verbatim */
func saveUploaded(r io.Reader, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

/* ── the VI normaliser ──────────────────────────────────────────────────── */
func normVI(src, crime string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	r := csv.NewReader(in)

	/* 1. scan pre-header lines for “MSISDN …” ------------------------------ */
	var header []string
	var cdr string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return "", errors.New("no header found")
		}
		if err != nil {
			continue
		}
		if cdr == "" {
			cdr = extractCdrNumber(strings.Join(rec, " "))
		}
		if colIdx(rec, "call date") != -1 { // found header row
			header = rec
			break
		}
	}

	/* 2. fallbacks for CDR -------------------------------------------------- */
	idxMSISDN := colIdxAny(header, "msisdn", "msisdn no", "msisdn number")
	firstData, err := r.Read()
	if err != nil {
		return "", errors.New("file has header but no data rows")
	}
	if cdr == "" && idxMSISDN != -1 && idxMSISDN < len(firstData) {
		cdr = nonDigit.ReplaceAllString(firstData[idxMSISDN], "")
	}
	if cdr == "" { // filename like 1234567890.csv ?
		nameDigits := nonDigit.ReplaceAllString(filepath.Base(src), "")
		if len(nameDigits) >= 10 && len(nameDigits) <= 15 {
			cdr = nameDigits
		}
	}
	if cdr == "" {
		return "", errors.New("CDR number (MSISDN) not found")
	}

	/* 3. locate all required columns --------------------------------------- */
	idxDate := colIdx(header, "call date")
	idxTime := colIdx(header, "call initiation time")
	idxDur := colIdxAny(header, "call duration", "duration")
	idxBparty := colIdxAny(header, "b party number", "b party no")
	idxType := colIdx(header, "call_type")
	idxFirstID := colIdxAny(header, "first cell global id")
	idxFirstAddr := colIdxAny(header, "first bts location")
	idxLastID := colIdxAny(header, "last cell global id")
	idxLastAddr := colIdxAny(header, "last bts location")
	idxIMEI := colIdx(header, "imei")
	idxIMSI := colIdx(header, "imsi")
	idxRoam := colIdxAny(header, "roaming network/circle", "roaming network")
	idxLRN := colIdxAny(header, "lrn- b party number", "lrn b party number")
	idxService := colIdx(header, "service type")

	/* 4. prepare output ----------------------------------------------------- */
	out := filepath.Join("filtered", cdr+"_reports.csv")
	outF, _ := os.Create(out)
	defer outF.Close()
	w := csv.NewWriter(outF)
	_ = w.Write(targetHeader)

	col := map[string]int{}
	for i, h := range targetHeader {
		col[h] = i
	}
	blank := make([]string, len(targetHeader))

	cp := func(rec []string, src int, dst string, row []string) {
		if src >= 0 && src < len(rec) {
			row[col[dst]] = strings.Trim(rec[src], "'\" ")
		}
	}

	/* helper to write one row */
	writeRow := func(rec []string) {
		if len(rec) == 0 {
			return
		}
		row := append([]string(nil), blank...)

		row[col["CdrNo"]] = cdr
		row[col["Crime"]] = crime

		cp(rec, idxDate, "Date", row)
		cp(rec, idxTime, "Time", row)
		cp(rec, idxDur, "Duration", row)
		cp(rec, idxBparty, "B Party", row)
		cp(rec, idxType, "Call Type", row)
		cp(rec, idxFirstID, "First Cell ID", row)
		cp(rec, idxFirstAddr, "First Cell ID Address", row)
		cp(rec, idxLastID, "Last Cell ID", row)
		cp(rec, idxLastAddr, "Last Cell ID Address", row)
		cp(rec, idxIMEI, "IMEI", row)
		cp(rec, idxIMSI, "IMSI", row)
		cp(rec, idxRoam, "Roaming", row)
		cp(rec, idxLRN, "LRN", row)
		cp(rec, idxService, "Type", row)

		/* enrich from vi_cells.csv */
		if firstID := pick(rec, idxFirstID); firstID != "" {
			if info, ok := findCell("vi", firstID); ok {
				row[col["Main City(First CellID)"]] = info.Main
				row[col["Sub City (First CellID)"]] = info.Sub
				row[col["Lat-Long-Azimuth (First CellID)"]] = info.LatLonAz
				if row[col["First Cell ID Address"]] == "" {
					row[col["First Cell ID Address"]] = info.Addr
				}
			}
		}

		/* provider / circle / operator via LRN */
		if l := nonDigit.ReplaceAllString(pick(rec, idxLRN), ""); l != "" {
			if info, ok := lrnDB[l]; ok {
				row[col["B Party Provider"]] = info.Provider
				row[col["B Party Circle"]] = info.Circle
				row[col["B Party Operator"]] = info.Operator
			}
		}

		w.Write(row)
	}

	/* 5. write firstData and rest of file ---------------------------------- */
	writeRow(firstData)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) == 0 {
			continue
		}
		writeRow(rec)
	}
	w.Flush()
	return out, w.Error()
}

/* ── generic column finder (case/space insensitive) ─────────────────────── */
func colIdx(header []string, key string) int {
	key = norm(key)
	for i, h := range header {
		if norm(h) == key {
			return i
		}
	}
	return -1
}
