package bsnl

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

/* ── canonical 26-column output header (DO NOT CHANGE ORDER) ────────────── */
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

func norm(s string) string {
	return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ")
}

func colIdxAny(header []string, keys ...string) int {
	for _, k := range keys {
		if idx := colIdx(header, k); idx != -1 {
			return idx
		}
	}
	return -1
}

func colIdx(header []string, key string) int {
	key = norm(key)
	for i, h := range header {
		if norm(h) == key {
			return i
		}
	}
	return -1
}

/* ── "Search Value : 94xxxxxxxx" banner → CDR number ────────────────────── */
var searchValRE = regexp.MustCompile(`(?i)search\s*value[^0-9]*([0-9]{8,15})`)

func extractCDR(line string) string {
	if m := searchValRE.FindStringSubmatch(line); len(m) > 1 {
		return m[1]
	}
	return ""
}

/* ── embedded reference CSVs ────────────────────────────────────────────── */
//go:embed data/*
var dataFS embed.FS

/* ------------- BSNL cell-site DB ------------------- */
type CellInfo struct{ Addr, Sub, Main, LatLonAz string }

var cellDB = map[string]CellInfo{}

/* ------------- LRN DB ------------------------------ */
type LRNInfo struct{ Provider, Circle, Operator string }

var lrnDB = map[string]LRNInfo{}

/* ── init: load reference data once at start --------- */
func init() {
	loadCells("data/bsnl_cells.csv")
	loadLRN("data/LRN.csv")
}

/* load bsnl_cells.csv */
func loadCells(path string) {
	f, err := dataFS.Open(path)
	if err != nil {
		log.Printf("warning: %s not found – cell enrichment disabled", path)
		return
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, _ := r.Read()

	iCGI := colIdxAny(header, "cgi", "cell id", "cell_id")
	iAddr := colIdxAny(header, "address")
	iSub := colIdxAny(header, "subcity", "sub city")
	iMain := colIdxAny(header, "maincity", "main city", "city")
	iLat := colIdxAny(header, "latitude", "lat")
	iLon := colIdxAny(header, "longitude", "lon", "long")
	iAz := colIdxAny(header, "azimuth", "azm", "az")

	if iCGI == -1 {
		log.Printf("warning: no CGI column in %s", path)
		return
	}
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
		cellDB[cgi] = CellInfo{
			Addr:     pick(rec, iAddr),
			Sub:      pick(rec, iSub),
			Main:     pick(rec, iMain),
			LatLonAz: buildLat(rec, iLat, iLon, iAz),
		}
		// digits-only key
		cellDB[nonDigit.ReplaceAllString(cgi, "")] = cellDB[cgi]
	}
}

/* load LRN.csv */
func loadLRN(path string) {
	f, err := dataFS.Open(path)
	if err != nil {
		log.Printf("warning: %s not found – provider/circle/operator blank", path)
		return
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, _ := r.Read()

	iLRN := colIdxAny(header, "lrn no", "lrn", "lrn number")
	iTSP := colIdxAny(header, "tsp", "provider", "tsp-lsa")
	iCircle := colIdxAny(header, "circle")
	if iLRN == -1 {
		log.Printf("warning: no LRN column in %s", path)
		return
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
			Operator: pick(rec, iTSP), // spec: Operator = tsp
		}
	}
}

/* ── misc helpers ───────────────────────────────────────────────────────── */
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
func cellLookup(id string) (CellInfo, bool) {
	if info, ok := cellDB[id]; ok {
		return info, true
	}
	return cellDB[nonDigit.ReplaceAllString(id, "")], cellDB[nonDigit.ReplaceAllString(id, "")].Addr != ""
}

/* ── HTTP upload handler ────────────────────────────────────────────────── */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	if strings.ToLower(r.FormValue("tsp_type")) != "bsnl" {
		http.Error(w, "Only BSNL supported here", 400)
		return
	}
	crime := r.FormValue("crime_number")

	// ---- store upload ----------------------------------------------------
	file, hdr, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer file.Close()
	_ = os.MkdirAll("uploads", 0o755)
	_ = os.MkdirAll("filtered", 0o755)
	src := filepath.Join("uploads", hdr.Filename)
	if err := save(file, src); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// ---- normalise -------------------------------------------------------
	out, err := normBSNL(src, crime)
	if err != nil {
		http.Error(w, "normalisation failed: "+err.Error(), 500)
		return
	}
	fmt.Fprintf(w, "/download/%s\n", filepath.Base(out))
}

func save(r io.Reader, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

/* ── core normaliser ───────────────────────────────────────────────────── */
func normBSNL(src, crime string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	r := csv.NewReader(in)

	/* 1. read until header row, extract CDR from banner -------------------- */
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
			cdr = extractCDR(strings.Join(rec, " "))
		}
		if colIdx(rec, "call_date") != -1 { // header located
			header = rec
			break
		}
	}

	/* 2. get first data row to ensure CDR fallback works ------------------ */
	firstData, err := r.Read()
	if err != nil {
		return "", errors.New("header present but file empty")
	}
	if cdr == "" {
		if idx := colIdxAny(header, "search value"); idx != -1 && idx < len(firstData) {
			cdr = nonDigit.ReplaceAllString(firstData[idx], "")
		}
	}
	if cdr == "" {
		cdr = nonDigit.ReplaceAllString(filepath.Base(src), "")
	}
	if cdr == "" {
		return "", errors.New("CDR number not found")
	}

	/* 3. locate every needed column -------------------------------------- */
	idxDate := colIdx(header, "call_date")
	idxTime := colIdxAny(header, "call_initiation_time(cit)", "call_initiation_time", "cit")
	idxDur := colIdx(header, "call_duration")
	idxBParty := colIdx(header, "other_party_no")
	idxType := colIdx(header, "call_type")
	idxFirstID := colIdx(header, "first_cell_id")
	idxLastID := colIdx(header, "last_cell_id")
	idxLastAddr := colIdx(header, "last_cell_desc")
	idxIMEI := colIdx(header, "imei")
	idxIMSI := colIdx(header, "imsi")
	idxRoam := colIdxAny(header, "roaming circle", "roaming_circle")
	idxLRN := colIdx(header, "lrn_b_party_no")
	idxService := colIdx(header, "service_type")

	/* 4. prepare output writer ------------------------------------------- */
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
		cp(rec, idxBParty, "B Party", row)
		cp(rec, idxType, "Call Type", row)
		cp(rec, idxFirstID, "First Cell ID", row)
		cp(rec, idxLastID, "Last Cell ID", row)
		cp(rec, idxLastAddr, "Last Cell ID Address", row)
		cp(rec, idxIMEI, "IMEI", row)
		cp(rec, idxIMSI, "IMSI", row)
		cp(rec, idxRoam, "Roaming", row)
		cp(rec, idxLRN, "LRN", row)
		cp(rec, idxService, "Type", row)

		/* cell enrichment (first cell) */
		if firstID := pick(rec, idxFirstID); firstID != "" {
			if info, ok := cellLookup(firstID); ok {
				row[col["First Cell ID Address"]] = info.Addr
				row[col["Main City(First CellID)"]] = info.Main
				row[col["Sub City (First CellID)"]] = info.Sub
				row[col["Lat-Long-Azimuth (First CellID)"]] = info.LatLonAz
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

	/* write first + remaining rows */
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
