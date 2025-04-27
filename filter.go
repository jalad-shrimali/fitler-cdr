package main

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

/* ──────────── embedded cell-ID lookup ──────────── */
//go:embed data/*
var cellFS embed.FS

type CellInfo struct {
	Address        string
	SubCity        string
	MainCity       string
	LatLongAzimuth string
}

var cellDB = map[string]CellInfo{}

func init() {
	f, err := cellFS.Open("data/airtel_cells.csv")
	if err != nil {
		log.Fatalf("cell lookup file missing: %v", err)
	}
	r := csv.NewReader(f)

	header, _ := r.Read()
	hIdx := map[string]int{}
	for i, h := range header {
		hIdx[norm(h)] = i
	}

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) == 0 {
			continue
		}
		id := strings.TrimSpace(rec[hIdx["cell id"]]) // 15-digit
		if id == "" {
			continue
		}
		cellDB[id] = CellInfo{
			Address:        rec[hIdx["address"]],
			SubCity:        rec[hIdx["subcity"]],
			MainCity:       rec[hIdx["maincity"]],
			LatLongAzimuth: rec[hIdx["latitude"]] + "," +
				rec[hIdx["longitude"]] + "," +
				rec[hIdx["azimuth"]],
		}
	}
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
	src := filepath.Join("uploads", hdr.Filename)
	dst := filepath.Join("filtered", "filtered_"+hdr.Filename)
	if err := saveUploaded(file, src); err != nil {
		http.Error(w, err.Error(), 500); return
	}

	switch tsp {
	case "airtel":
		if err := normalizeAirtel(src, dst, crime, tsp); err != nil {
			http.Error(w, "normalization failed: "+err.Error(), 500); return
		}
	default:
		http.Error(w, "unsupported tsp_type", 400); return
	}

	fmt.Fprintf(w, "Normalized file created: /download/%s\n", filepath.Base(dst))
}

func saveUploaded(src io.Reader, dst string) error {
	f, err := os.Create(dst); if err != nil { return err }
	defer f.Close()
	_, err = io.Copy(f, src)
	return err
}

/* ──────────── Airtel normalizer ──────────── */

func cleanCGI(raw string) string { return strings.ReplaceAll(raw, "-", "") }

func enrichWithCell(row []string, col map[string]int, id string, first bool) {
	info, ok := cellDB[id]
	if !ok {
		return
	}
	if first {
		row[col["First Cell ID Address"]]            = info.Address
		row[col["Sub City (First CellID)"]]          = info.SubCity
		row[col["Main City(First CellID)"]]          = info.MainCity
		row[col["Lat-Long-Azimuth (First CellID)"]]  = info.LatLongAzimuth
	} else {
		row[col["Last Cell ID Address"]] = info.Address
	}
}

func normalizeAirtel(src, dst, crime, tsp string) error {
	in, err := os.Open(src)
	if err != nil { return err }
	defer in.Close()
	r := csv.NewReader(in)

	/* ─── locate header + CDR number ─── */
	var header []string
	var cdrNumber string
	for {
		rec, err := r.Read()
		if err == io.EOF { return fmt.Errorf("no header found") }
		if err != nil { continue }

		if cdrNumber == "" && len(rec) > 0 {
			cdrNumber = extractCdrNumber(tsp, rec[0])
		}
		if len(rec) > 0 && strings.Contains(rec[0], "Target No") {
			header = rec; break
		}
	}
	if cdrNumber == "" { return fmt.Errorf("could not extract CDR number") }

	/* ─── build column maps ─── */
	srcToDst := map[int]int{}
	col      := map[string]int{}        // every canonical column → index
	for i, name := range targetHeader { col[name] = i }

	// first / last CGI in input
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
		return fmt.Errorf("CSV lacks First/Last CGI columns")
	}
	srcToDst[firstCGI] = col["First Cell ID"]
	srcToDst[lastCGI]  = col["Last Cell ID"]

	/* ─── output setup ─── */
	out, _ := os.Create(dst)
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

		// enrich with lookup
		enrichWithCell(row, col, row[col["First Cell ID"]], true)
		enrichWithCell(row, col, row[col["Last Cell ID"]],  false)

		if err := w.Write(row); err != nil { return err }
	}
	w.Flush()
	return w.Error()
}
