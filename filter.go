package main

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
	"strconv"
	"strings"
	"time"
)

/* ───────── canonical 26‑column layout for filtered output ───────── */
var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Main City(First CellID)", "Sub City (First CellID)", "Lat-Long-Azimuth (First CellID)",
	"Crime", "Circle", "Operator", "LRN",
=======
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

/* ───────── helpers ───────── */
var (
	spaceRE  = regexp.MustCompile(`\s+`)
	nonDigit = regexp.MustCompile(`\D`)
)
func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }
func digits(s string) string { return nonDigit.ReplaceAllString(s, "") }
func last10(s string) string { d := digits(s); if len(d) > 10 { return d[len(d)-10:] }; return d }
func cleanCGI(s string) string { return digits(s) }

/* column index helpers */
func colIdxAny(header []string, keys ...string) int {
	for _, k := range keys {
		for i, h := range header {
			if norm(h) == norm(k) {
				return i
			}
		}
	}
	return -1
} 
func colIdx(header []string, key string) int { return colIdxAny(header, key) }

/* ───────── banner‑line CDR extractor ───────── */
var jioCdrRE = regexp.MustCompile(`(?i)input value[^0-9]*([0-9]{8,15})`)
func extractCdrNumber(line string) string {
	if m := jioCdrRE.FindStringSubmatch(line); len(m) > 1 { return m[1] }
	return ""
}

/* ───────── embedded lookup data ───────── */
//go:embed data/*
var dataFS embed.FS

type CellInfo struct{ Addr, Sub, Main, LatLonAz string }
type LRNInfo  struct{ Provider, Circle, Operator string }

var (
	cellDB = map[string]map[string]CellInfo{} // tsp → map[cellID]info
	lrnDB  = map[string]LRNInfo{}             // digits(LRN) → info
)

func init() {
	if err := loadCells("jio", "data/jio_cells.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("cell DB: %v", err)
	}
	if err := loadLRN("data/LRN.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("LRN.csv not loaded: %v (provider/circle columns will be blank)", err)
	}
}

/* ---------- loadCells ---------- */
func loadCells(tsp, path string) error {
	f, err := dataFS.Open(path)
	if err != nil { return err }
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read(); if err != nil { return err }
	idx := func(keys ...string) int { return colIdxAny(header, keys...) }

	iID := idx("cgi", "cell id")
	iAddr, iSub := idx("address"), idx("subcity", "sub city")
	iMain := idx("maincity", "city")
	iLat, iLon, iAz := idx("latitude"), idx("longitude", "lon"), idx("azimuth", "az")
	if iID == -1 { return fmt.Errorf("no CGI column in %s", path) }

	cellDB[tsp] = map[string]CellInfo{}
	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }

		raw := strings.TrimSpace(rec[iID]); if raw == "" { continue }
		info := CellInfo{
			Addr:     pick(rec, iAddr),
			Sub:      pick(rec, iSub),
			Main:     pick(rec, iMain),
			LatLonAz: buildLat(rec, iLat, iLon, iAz),
		}
		cellDB[tsp][raw]        = info
		cellDB[tsp][digits(raw)] = info
	}
	return nil
}

/* ---------- loadLRN ---------- */
func loadLRN(path string) error {
	f, err := dataFS.Open(path)
	if err != nil { return err }
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read(); if err != nil { return err }

	iLRN   := colIdxAny(header, "lrn", "lrn no")
	iTSP   := colIdxAny(header, "tsp", "provider")
	iCircle:= colIdx(header, "circle")
	if iLRN == -1 || iTSP == -1 { return fmt.Errorf("LRN.csv missing columns") }

	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }

		key := digits(rec[iLRN]); if key == "" { continue }
		lrnDB[key] = LRNInfo{
			Provider: pick(rec, iTSP),
			Circle:   pick(rec, iCircle),
			Operator: pick(rec, iTSP), // requirement: operator == provider
		}
	}
	return nil
}

/* misc helpers */
func pick(rec []string, idx int) string {
	if idx == -1 || idx >= len(rec) { return "" }
	return strings.TrimSpace(rec[idx])
}
func buildLat(rec []string, iLat, iLon, iAz int) string {
	if iLat == -1 || iLon == -1 { return "" }
	lat, lon := pick(rec, iLat), pick(rec, iLon)
	if lat == "" || lon == "" { return "" }
	if az := pick(rec, iAz); az != "" { return lat + ", " + lon + ", " + az }
	return lat + ", " + lon
}
func findCell(tsp, id string) (CellInfo, bool) {
	db := cellDB[tsp]
	if info, ok := db[id]; ok { return info, true }
	if info, ok := db[digits(id)]; ok { return info, true }
	return CellInfo{}, false
}

/* ───────────────────── HTTP handler ───────────────────── */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "POST only", 405); return }
	if strings.ToLower(r.FormValue("tsp_type")) != "jio" {
		http.Error(w, "Only Jio supported", 400); return
	}
	crime := r.FormValue("crime_number")

	fh, hdr, err := r.FormFile("file")
	if err != nil { http.Error(w, err.Error(), 400); return }
	defer fh.Close()

	_ = os.MkdirAll("uploads", 0o755)
	_ = os.MkdirAll("filtered", 0o755)

	src := filepath.Join("uploads", hdr.Filename)
	if err := saveUploaded(fh, src); err != nil { http.Error(w, err.Error(), 500); return }

	filtered, summary, err := normJio(src, crime)
	if err != nil { http.Error(w, err.Error(), 500); return }

	fmt.Fprintf(w, "/download/%s\n/download/%s\n", filepath.Base(filtered), filepath.Base(summary))
}

func saveUploaded(r io.Reader, dst string) error {
	f, err := os.Create(dst); if err != nil { return err }
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

/* ───────── enrichment for cell address columns ───────── */
func enrich(row []string, col map[string]int, id string, first bool) {
	if info, ok := findCell("jio", id); ok {
		if first {
			row[col["First Cell ID Address"]]          = info.Addr
			row[col["Sub City (First CellID)"]]        = info.Sub
			row[col["Main City(First CellID)"]]        = info.Main
			row[col["Lat-Long-Azimuth (First CellID)"]] = info.LatLonAz
		} else {
			row[col["Last Cell ID Address"]] = info.Addr
		}
	}
}

/* ─────────────────── Jio normaliser (filtered + per‑party summary) ───── */
func normJio(src, crime string) (filteredPath, summaryPath string, err error) {
	in, err := os.Open(src); if err != nil { return "", "", err }
	defer in.Close()
	r := csv.NewReader(in)

	/* ── locate header row & essential indexes ── */
	var (
		header                                                []string
		cdr                                                   string
		iFirst, iLast, iCalling, iCalled, iInput             = -1, -1, -1, -1, -1
	)
	for {
		rec, er := r.Read()
		if er == io.EOF { return "", "", errors.New("no header") }
		if er != nil { continue }

		if cdr == "" { cdr = extractCdrNumber(strings.Join(rec, " ")) }

		for i, h := range rec {
			switch norm(h) {
			case "first cgi", "first cell id": iFirst = i
			case "last cgi",  "last cell id":  iLast  = i
			case "calling party telephone number": iCalling = i
			case "called party telephone number":  iCalled  = i
			}
			if strings.Contains(strings.ToLower(h), "input value") { iInput = i }
		}
		if iFirst != -1 && iLast != -1 { header = rec; break }
	}

	/* fallback CDR from first data line, if needed */
	var firstRec []string
	if cdr == "" && iInput != -1 {
		firstRec, _ = r.Read()
		if len(firstRec) > iInput {
			if m := regexp.MustCompile(`\d{8,15}`).FindString(firstRec[iInput]); m != "" { cdr = m }
		}
	}
	if cdr == "" { return "", "", errors.New("CDR not found") }
	cdr10 := last10(cdr)

	/* -------- filtered writer -------- */
	filteredPath = filepath.Join("filtered", cdr+"_reports.csv")
	fout, _ := os.Create(filteredPath)
	defer fout.Close()
	fw := csv.NewWriter(fout)
	_ = fw.Write(targetHeader)

	col := make(map[string]int)
	for i, h := range targetHeader { col[h] = i }
	blank := make([]string, len(targetHeader))

	/* -------- multi‑party summary aggregator -------- */
	type agg struct {
		BParty, SDR, Provider, Type           string
		TotalCalls, OutCalls, InCalls         int
		OutSMS, InSMS, OtherCalls             int
		RoamCalls, RoamSMS                    int
		TotalDuration                         float64
		Days, CellIds, Imeis, Imsis           map[string]struct{}
		FirstCall, LastCall                   string
	}
	summary := map[string]*agg{}
	timeLayout := "2006-01-02 15:04:05"
	parseDT := func(d, t string) string {
		dt := strings.TrimSpace(d) + " " + strings.TrimSpace(t)
		if _, e := time.Parse(timeLayout, dt); e == nil { return dt }
		return dt
	}

	cp := func(rec []string, src int, dst string, row []string) {
		if src >= 0 && src < len(rec) { row[col[dst]] = strings.Trim(rec[src], "'\" ") }
	}

	writeRow := func(rec []string) {
		if len(rec) == 0 { return }

		row := append([]string(nil), blank...)
		row[col["CdrNo"]] = cdr

		iDate := colIdx(header, "call date")
		iTime := colIdx(header, "call time")
		iDur  := colIdxAny(header, "dur(s)", "duration(sec)", "call duration")
		iIMEI := colIdx(header, "imei")
		iIMSI := colIdx(header, "imsi")
		iLRN  := colIdxAny(header, "lrn called no", "lrn no", "lrn")
		iRoam := colIdx(header, "roaming circle name")
		iCT   := colIdx(header, "call type")

		cp(rec, iDate, "Date", row); cp(rec, iTime, "Time", row)
		cp(rec, iDur,  "Duration", row)
		cp(rec, iIMEI, "IMEI", row); cp(rec, iIMSI, "IMSI", row)
		cp(rec, iLRN,  "LRN",  row)
		cp(rec, colIdxAny(header, "call forward", "call fwd no", "call fow no"), "CallForward", row)
		cp(rec, iRoam, "Roaming", row)

		switch ct := strings.ToUpper(strings.Trim(rec[iCT], "'\" ")); ct {
		case "A_IN", "CALL_IN":  row[col["Call Type"]] = "CALL_IN"
		case "A_OUT", "CALL_OUT": row[col["Call Type"]] = "CALL_OUT"
		default: row[col["Call Type"]] = ct
		}
		row[col["Crime"]] = crime

		firstID := cleanCGI(rec[iFirst]); lastID := cleanCGI(rec[iLast])
		row[col["First Cell ID"]] = firstID; row[col["Last Cell ID"]] = lastID
		enrich(row, col, firstID, true); enrich(row, col, lastID, false)

		callRaw, calledRaw := strings.Trim(rec[iCalling], "'\" "), strings.Trim(rec[iCalled], "'\" ")
		switch {
		case last10(callRaw) == cdr10 && calledRaw != "": row[col["B Party"]] = calledRaw
		case last10(calledRaw) == cdr10 && callRaw != "": row[col["B Party"]] = callRaw
		default:
			if calledRaw != "" { row[col["B Party"]] = calledRaw } else { row[col["B Party"]] = callRaw }
		}
		bKey := row[col["B Party"]]
		if bKey == "" { bKey = "(blank)" }

		if info, ok := lrnDB[digits(row[col["LRN"]])]; ok {
			row[col["B Party Provider"]] = info.Provider
			row[col["B Party Circle"]]   = info.Circle
			row[col["B Party Operator"]] = info.Operator
		}

		fw.Write(row)

		/* ---- update / create aggregator ---- */
		a, ok := summary[bKey]
		if !ok {
			a = &agg{
				BParty: bKey, SDR: row[col["B Party Operator"]],
				Provider: row[col["B Party Provider"]],
				Type: row[col["Type"]],
				Days: map[string]struct{}{}, CellIds: map[string]struct{}{},
				Imeis: map[string]struct{}{}, Imsis: map[string]struct{}{},
			}
			summary[bKey] = a
		}

		a.TotalCalls++
		switch row[col["Call Type"]] {
		case "CALL_OUT": a.OutCalls++
		case "CALL_IN":  a.InCalls++
		default:
			if strings.Contains(row[col["Call Type"]], "SMS") {
				if strings.HasSuffix(row[col["Call Type"]], "OUT") { a.OutSMS++ } else { a.InSMS++ }
			} else { a.OtherCalls++ }
		}
		if row[col["Roaming"]] != "" {
			if strings.Contains(row[col["Call Type"]], "SMS") { a.RoamSMS++ } else { a.RoamCalls++ }
		}
		if dur, er := strconv.ParseFloat(row[col["Duration"]], 64); er == nil { a.TotalDuration += dur }

		a.Days[row[col["Date"]]] = struct{}{}
		if firstID != "" { a.CellIds[firstID] = struct{}{} }
		if lastID  != "" { a.CellIds[lastID]  = struct{}{} }
		if v := row[col["IMEI"]]; v != "" { a.Imeis[v] = struct{}{} }
		if v := row[col["IMSI"]]; v != "" { a.Imsis[v] = struct{}{} }

		dt := parseDT(row[col["Date"]], row[col["Time"]])
		if a.FirstCall == "" || dt < a.FirstCall { a.FirstCall = dt }
		if a.LastCall  == "" || dt > a.LastCall  { a.LastCall  = dt }
	}

	/* -------- iterate through CSV -------- */
	if len(firstRec) > 0 { writeRow(firstRec) }
	for {
		rec, er := r.Read()
		if er == io.EOF { break }
		if er != nil || len(rec) == 0 { continue }
		writeRow(rec)
	}
	fw.Flush()

	/* -------- write summary file -------- */
	summaryPath = filepath.Join("filtered", cdr+"_summary_reports.csv")
	sout, _ := os.Create(summaryPath)
	defer sout.Close()
	sw := csv.NewWriter(sout)
	sw.Write([]string{
		"CdrNo", "B Party", "B Party SDR", "Provider", "Type",
		"Total Calls", "Out Calls", "In Calls", "Out Sms", "In Sms",
		"Other Calls", "Roam Calls", "Roam Sms", "Total Duration",
		"Total Days", "Total CellIds", "Total Imei", "Total Imsi",
		"First Call", "Last Call",
	})
	for _, a := range summary {
		sw.Write([]string{
			cdr, a.BParty, a.SDR, a.Provider, a.Type,
			fmt.Sprint(a.TotalCalls), fmt.Sprint(a.OutCalls), fmt.Sprint(a.InCalls),
			fmt.Sprint(a.OutSMS), fmt.Sprint(a.InSMS), fmt.Sprint(a.OtherCalls),
			fmt.Sprint(a.RoamCalls), fmt.Sprint(a.RoamSMS),
			fmt.Sprintf("%.0f", a.TotalDuration),
			fmt.Sprint(len(a.Days)), fmt.Sprint(len(a.CellIds)),
			fmt.Sprint(len(a.Imeis)), fmt.Sprint(len(a.Imsis)),
			a.FirstCall, a.LastCall,
		})
	}
	sw.Flush()

	return filteredPath, summaryPath, nil
}

/* ─────────────────────────────────────────────────────────── */
=======
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
