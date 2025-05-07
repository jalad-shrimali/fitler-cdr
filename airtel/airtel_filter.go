package airtel
// code for airtel cdr normalization
import (
	"embed"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

/* ──────────── canonical 26-column layout (keep order) ─────────────── */

var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming", "Main City(First CellID)", "Sub City (First CellID)",
	"Lat-Long-Azimuth (First CellID)", "Crime", "Circle", "Operator", "LRN",
	"CallForward", "B Party Provider", "B Party Circle", "B Party Operator",
	"Type", "IMEI Manufacturer",
}

/* ──────────── column-name synonyms (trim-and-lowered) ─────────────── */

var synonyms = map[string]string{
	"b party no": "B Party", "called party telephone number": "B Party",
	"date": "Date", "call date": "Date",
	"time": "Time", "call time": "Time",
	"dur(s)": "Duration", "call duration": "Duration",
	"call type": "Call Type",
	"imei": "IMEI", "imsi": "IMSI",
	"roam nw": "Roaming", "roaming circle name": "Circle",
	"circle": "Circle", "operator": "Operator",
	"lrn": "LRN", "lrn called no": "LRN",
	"call fow no": "CallForward", "call forwarding": "CallForward",
	"lrn tsp-lsa": "B Party Provider", "b party provider": "B Party Provider",
	"b party circle": "B Party Circle", "b party operator": "B Party Operator",
	"service type": "Type",
	"crime": "Crime",
	"lrn no": "LRN", "lrn number": "LRN", "lrn no.": "LRN",
}

/* ──────────── helpers ──────────── */

var spaceRE = regexp.MustCompile(`\s+`)
func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }

/* ──────────── CDR-number extraction ──────────── */

var (
	airtelCdrRE = regexp.MustCompile(`Mobile No '(\d+)'`)
	jioCdrRE    = regexp.MustCompile(`Input Value : (\d+)`)
	viCdrRE     = regexp.MustCompile(`MSISDN : - (\d+)`)
)

func extractCdrNumber(tsp, content string) string {
	switch strings.ToLower(tsp) {
	case "airtel":
		if m := airtelCdrRE.FindStringSubmatch(content); len(m) > 1 { return m[1] }
	case "jio":
		if m := jioCdrRE.FindStringSubmatch(content); len(m) > 1 { return m[1] }
	case "vi":
		if m := viCdrRE.FindStringSubmatch(content); len(m) > 1 { return m[1] }
	}
	return ""
}

/* ──────────── embedded lookup files ──────────── */

// go:embed data/*
var dataFS embed.FS

/* ─── Cell-ID lookup ─── */

type CellInfo struct {
	Address, SubCity, MainCity, LatLongAzimuth string
}

var cellDB = map[string]CellInfo{}

/* ─── LRN lookup ─── */

type LRNInfo struct {
	Provider string // tsp
	Circle   string
	Operator string // if “operator” column missing, fall back to tsp
}

var lrnDB = map[string]LRNInfo{}

func init() {
	/* ── airtel_cells.csv ── */
	cf, err := dataFS.Open("data/airtel_cells.csv")
	if err != nil {
		log.Fatalf("cell lookup file missing: %v", err)
	}
	loadCells(cf)

	/* ── LRN.csv ── (best-effort) */
	lf, err := dataFS.Open("data/LRN.csv")
	if err != nil {
		log.Printf("LRN lookup file missing: %v (B Party fields will stay blank)", err)
		return
	}
	loadLRN(lf)
}

func loadCells(f io.Reader) {
	r := csv.NewReader(f)
	header, _ := r.Read()
	h := indexMap(header)

	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }
		id := strings.TrimSpace(rec[h["cell id"]])
		if id == "" { continue }
		cellDB[id] = CellInfo{
			Address:        rec[h["address"]],
			SubCity:        rec[h["subcity"]],
			MainCity:       rec[h["maincity"]],
			LatLongAzimuth: rec[h["latitude"]] + "," + rec[h["longitude"]] + "," + rec[h["azimuth"]],
		}
	}
}

func loadLRN(f io.Reader) {
	r := csv.NewReader(f)
	header, _ := r.Read()
	h := indexMap(header)

	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }

		key := strings.TrimSpace(rec[h["lrn no"]])
		if key == "" { continue }

		opCol, ok := h["operator"]
		operator := ""
		if ok { operator = rec[opCol] }

		lrnDB[key] = LRNInfo{
			Provider: rec[h["tsp"]],
			Circle:   rec[h["circle"]],
			Operator: operator,
		}
	}
}

func indexMap(header []string) map[string]int {
	m := map[string]int{}
	for i, h := range header { m[norm(h)] = i }
	return m
}

/* ──────────── HTTP endpoint ──────────── */

func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed); return
	}
	tsp   := strings.ToLower(r.FormValue("tsp_type"))
	crime := r.FormValue("crime_number")

	file, hdr, err := r.FormFile("file")
	if err != nil { http.Error(w, err.Error(), 400); return }
	defer file.Close()

	for _, d := range []string{"uploads", "filtered"} {
		if err := os.MkdirAll(d, 0755); err != nil {
			http.Error(w, err.Error(), 500); return
		}
	}
	srcPath := filepath.Join("uploads", hdr.Filename)
	if err := saveUploaded(file, srcPath); err != nil {
		http.Error(w, err.Error(), 500); return
	}

	var dstPath string
	switch tsp {
	case "airtel":
		dstPath, err = normalizeAirtel(srcPath, "filtered", crime, tsp)
		if err != nil {
			http.Error(w, "normalization failed: "+err.Error(), 500); return
		}
	default:
		http.Error(w, "unsupported tsp_type", 400); return
	}

	fmt.Fprintf(w, "Normalized file created: /download/%s\n", filepath.Base(dstPath))
}

func saveUploaded(src io.Reader, dst string) error {
	f, err := os.Create(dst)
	if err != nil { return err }
	defer f.Close()
	_, err = io.Copy(f, src)
	return err
}

/* ──────────── Airtel normalizer ──────────── */

func cleanCGI(raw string) string { return strings.ReplaceAll(raw, "-", "") }

func enrichWithCell(row []string, col map[string]int, id string, first bool) {
	info, ok := cellDB[id]
	if !ok { return }
	if first {
		row[col["First Cell ID Address"]]           = info.Address
		row[col["Sub City (First CellID)"]]         = info.SubCity
		row[col["Main City(First CellID)"]]         = info.MainCity
		row[col["Lat-Long-Azimuth (First CellID)"]] = info.LatLongAzimuth
	} else {
		row[col["Last Cell ID Address"]] = info.Address
	}
}

func enrichWithLRN(row []string, col map[string]int) {
	lrn := strings.TrimSpace(row[col["LRN"]])
	if lrn == "" { return }
	info, ok := lrnDB[lrn]
	if !ok { return }

	if row[col["B Party Provider"]] == "" { row[col["B Party Provider"]] = info.Provider }
	row[col["B Party Circle"]]   = info.Circle
	if info.Operator != "" {
		row[col["B Party Operator"]] = info.Operator
	} else {
		row[col["B Party Operator"]] = info.Provider
	}
}

/*  normalizeAirtel now RETURNS the path of the file it wrote  */
func normalizeAirtel(src, dstDir, crime, tsp string) (string, error) {
	in, err := os.Open(src)
	if err != nil { return "", err }
	defer in.Close()
	r := csv.NewReader(in)

	/* ─── locate header + CDR number ─── */
	var header []string
	var cdrNumber string
	for {
		rec, err := r.Read()
		if err == io.EOF { return "", fmt.Errorf("no header found") }
		if err != nil { continue }

		if cdrNumber == "" && len(rec) > 0 {
			cdrNumber = extractCdrNumber(tsp, rec[0])
		}
		if len(rec) > 0 && strings.Contains(rec[0], "Target No") {
			header = rec; break
		}
	}
	if cdrNumber == "" { return "", fmt.Errorf("could not extract CDR number") }

	/* ─── build column maps ─── */
	srcToDst := map[int]int{}
	col      := map[string]int{} // canonical → index
	for i, name := range targetHeader { col[name] = i }

	firstCGI, lastCGI := -1, -1
	for i, h := range header {
		hNorm := norm(h)
		if hNorm == "first cgi" { firstCGI = i }
		if hNorm == "last cgi"  { lastCGI  = i }

		if canon, ok := synonyms[hNorm]; ok {
			srcToDst[i] = col[canon]
		}
	}
	if firstCGI == -1 || lastCGI == -1 {
		return "", fmt.Errorf("CSV lacks First/Last CGI columns")
	}
	srcToDst[firstCGI] = col["First Cell ID"]
	srcToDst[lastCGI]  = col["Last Cell ID"]

	/* ─── output setup ─── */
	outPath := filepath.Join(dstDir, fmt.Sprintf("%s_reports.csv", cdrNumber))
	out, err := os.Create(outPath)
	if err != nil { return "", err }
	defer out.Close()
	w := csv.NewWriter(out)
	_ = w.Write(targetHeader)
	blank := make([]string, len(targetHeader))

	skip := func(s string) bool {
		s = strings.ToLower(strings.TrimSpace(s))
		return strings.HasPrefix(s, "this is system") || strings.Contains(s, "system generated")
	}

	/* ─── stream records ─── */
	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 || skip(rec[0]) { continue }

		row := append([]string(nil), blank...)
		row[col["CdrNo"]] = cdrNumber

		// copy simple fields
		for s, d := range srcToDst {
			if s >= len(rec) { continue }
			v := strings.Trim(rec[s], "'\" ")
			switch targetHeader[d] {
			case "Call Type":
				switch strings.ToUpper(v) {
				case "IN", "A_IN":  v = "CALL_IN"
				case "OUT", "A_OUT": v = "CALL_OUT"
				}
			case "Type":
				if strings.EqualFold(v, "pre")  { v = "Prepaid" }
				if strings.EqualFold(v, "post") { v = "Postpaid" }
			}
			row[d] = v
		}

		row[col["Crime"]] = crime

		// clean & write CGI numbers
		if first := cleanCGI(rec[firstCGI]); first != "" {
			row[col["First Cell ID"]] = first
		}
		if last := cleanCGI(rec[lastCGI]); last != "" {
			row[col["Last Cell ID"]] = last
		}

		// enrich look-ups
		enrichWithCell(row, col, row[col["First Cell ID"]], true)
		enrichWithCell(row, col, row[col["Last Cell ID"]],  false)
		enrichWithLRN(row,  col)

		if err := w.Write(row); err != nil { return "", err }
	}
	w.Flush()
	return outPath, w.Error()
}