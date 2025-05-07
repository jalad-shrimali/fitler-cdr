package jio
// code for jio cdr normalization

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

/* ── canonical 26-column layout (keep order) ─────────────────────────────── */
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

/* ── helpers ─────────────────────────────────────────────────────────────── */
var (
	spaceRE  = regexp.MustCompile(`\s+`)
	nonDigit = regexp.MustCompile(`\D`)
)

func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }

/* first matching column index out of several candidate names */
func colIdxAny(header []string, keys ...string) int {
	for _, k := range keys {
		if idx := colIdx(header, k); idx != -1 {
			return idx
		}
	}
	return -1
}

/* digits-only, keep last 10 digits so +91xxx… matches 10-digit CDR */
func normalizeMSISDN(s string) string {
	d := nonDigit.ReplaceAllString(s, "")
	if len(d) > 10 {
		return d[len(d)-10:]
	}
	return d
}

/* ── banner-line CDR extractor ───────────────────────────────────────────── */
var jioCdrRE = regexp.MustCompile(`(?i)input value[^0-9]*([0-9]{8,15})`)

func extractCdrNumber(line string) string {
	if m := jioCdrRE.FindStringSubmatch(line); len(m) > 1 {
		return m[1]
	}
	return ""
}

/* ── embedded data ───────────────────────────────────────────────────────── */
// go:embed data/*
var cellFS embed.FS

/* ---------- Cell-ID DB ---------- */
type CellInfo struct{ Addr, Sub, Main, LatLonAz string }

var cellDB = map[string]map[string]CellInfo{}

/* ---------- LRN DB ---------- */
type LRNInfo struct{ Provider, Circle, Operator string }

var lrnDB = map[string]LRNInfo{}

func init() {
	/* cell data */
	if err := loadCells("jio", "data/jio_cells.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("load jio cells: %v", err)
	}
	/* LRN data */
	if err := loadLRN("data/LRN.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("warning: could not load LRN.csv: %v (provider/circle/operator columns will stay blank)", err)
	}
}

/* ── load Jio cell database ─────────────────────────────────────────────── */
func loadCells(tsp, path string) error {
	f, err := cellFS.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return err
	}
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
	idxID := col("cgi", "cell id", "cellid")
	idxAddr := col("address")
	idxSub := col("subcity", "sub city")
	idxMain := col("maincity", "main city", "city")
	idxLat := col("latitude", "lat")
	idxLon := col("longitude", "lon", "long")
	idxAz := col("azimuth", "azm", "az")
	if idxID == -1 {
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
		rawID := strings.TrimSpace(rec[idxID])
		if rawID == "" {
			continue
		}
		digitsID := cleanCGI(rawID)
		info := CellInfo{
			Addr:     pick(rec, idxAddr),
			Sub:      pick(rec, idxSub),
			Main:     pick(rec, idxMain),
			LatLonAz: buildLat(rec, idxLat, idxLon, idxAz),
		}
		cellDB[tsp][rawID] = info
		cellDB[tsp][digitsID] = info
	}
	return nil
}

/* ── load LRN.csv ───────────────────────────────────────────────────────── */
func loadLRN(path string) error {
	f, err := cellFS.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return err
	}
	idxLRN := colIdxAny(header, "lrn no", "lrn", "lrn number")
	idxTSP := colIdxAny(header, "tsp", "provider", "tsp-lsa")
	idxCircle := colIdxAny(header, "circle")
	if idxLRN == -1 {
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
		lrn := nonDigit.ReplaceAllString(rec[idxLRN], "")
		if lrn == "" {
			continue
		}
		lrnDB[lrn] = LRNInfo{
			Provider: pick(rec, idxTSP),
			Circle:   pick(rec, idxCircle),
			Operator: pick(rec, idxTSP), // per requirement: Operator = tsp
		}
	}
	return nil
}

/* ── misc small helpers ─────────────────────────────────────────────────── */
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
func cleanCGI(s string) string { return nonDigit.ReplaceAllString(s, "") }
func findCell(tsp, id string) (CellInfo, bool) {
	db := cellDB[tsp]
	if info, ok := db[id]; ok {
		return info, true
	}
	for l := len(id) - 1; l >= 11; l-- {
		if info, ok := db[id[:l]]; ok {
			return info, true
		}
	}
	if info, ok := db["0"+id]; ok {
		return info, true
	}
	if len(id) > 0 && id[0] == '0' {
		if info, ok := db[id[1:]]; ok {
			return info, true
		}
	}
	return CellInfo{}, false
}

/* ── HTTP endpoint ───────────────────────────────────────────────────────── */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	tsp := strings.ToLower(r.FormValue("tsp_type"))
	if tsp != "jio" {
		http.Error(w, "Only Jio supported", 400)
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
	out, err := normJio(src, crime)
	if err != nil {
		http.Error(w, "normalization failed: "+err.Error(), 500)
		return
	}
	fmt.Fprintf(w, "/download/%s\n", filepath.Base(out))
}

func saveUploaded(r io.Reader, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

/* ── enrich row with cell-address data ───────────────────────────────────── */
func enrich(row []string, col map[string]int, id string, first bool, tsp string) {
	if info, ok := findCell(tsp, id); ok {
		if first {
			row[col["First Cell ID Address"]] = info.Addr
			row[col["Sub City (First CellID)"]] = info.Sub
			row[col["Main City(First CellID)"]] = info.Main
			row[col["Lat-Long-Azimuth (First CellID)"]] = info.LatLonAz
		} else {
			row[col["Last Cell ID Address"]] = info.Addr
		}
	}
}

/* ── Jio CSV normaliser ─────────────────────────────────────────────────── */
func normJio(src, crime string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	r := csv.NewReader(in)

	var (
		header                                []string
		cdr                                   string
		idxFirst, idxLast                     int
		idxCalling, idxCalled                 int
		idxInput                              int
	)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return "", errors.New("no header")
		}
		if err != nil {
			continue
		}
		if cdr == "" {
			cdr = extractCdrNumber(strings.Join(rec, " "))
		}
		for i, h := range rec {
			switch norm(h) {
			case "first cgi", "first cell id":
				idxFirst = i
			case "last cgi", "last cell id":
				idxLast = i
			case "calling party telephone number":
				idxCalling = i
			case "called party telephone number":
				idxCalled = i
			}
			if strings.Contains(strings.ToLower(h), "input value") {
				idxInput = i
			}
		}
		if idxFirst != 0 && idxLast != 0 {
			header = rec
			break
		}
	}

	/* fallback: banner-style CDR */
	var firstRec []string
	if cdr == "" && idxInput != -1 {
		firstRec, _ = r.Read()
		if len(firstRec) > idxInput {
			if m := regexp.MustCompile(`\d{8,15}`).FindString(firstRec[idxInput]); m != "" {
				cdr = m
			}
		}
	}
	if cdr == "" {
		return "", errors.New("CDR not found")
	}

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

	/* helper: copy into canonical row */
	cp := func(rec []string, srcIdx int, dst string, row []string) {
		if srcIdx >= 0 && srcIdx < len(rec) {
			row[col[dst]] = strings.Trim(rec[srcIdx], "'\" ")
		}
	}

	write := func(rec []string) {
		if len(rec) == 0 {
			return
		}
		row := append([]string(nil), blank...)
		row[col["CdrNo"]] = cdr

		/* basic copies */
		cp(rec, colIdx(header, "call date"), "Date", row)
		cp(rec, colIdx(header, "call time"), "Time", row)
		cp(rec, colIdxAny(header, "dur(s)", "duration(sec)", "call duration"), "Duration", row)
		cp(rec, colIdx(header, "imei"), "IMEI", row)
		cp(rec, colIdx(header, "imsi"), "IMSI", row)

		/* LRN and CallForward */
		lrnRaw := rec[colIdxAny(header, "lrn called no", "lrn no", "lrn")]
		cp(rec, colIdxAny(header, "lrn called no", "lrn no", "lrn"), "LRN", row)
		cp(rec, colIdxAny(header, "call forward", "call forwarding", "call fwd no", "call fow no"), "CallForward", row)

		/* call / sms type */
		ct := strings.ToUpper(strings.Trim(rec[colIdx(header, "call type")], "'\" "))
		switch ct {
		case "A_IN", "CALL_IN":
			row[col["Call Type"]] = "CALL_IN"
			row[col["Type"]] = "Phone"
		case "A_OUT", "CALL_OUT":
			row[col["Call Type"]] = "CALL_OUT"
			row[col["Type"]] = "Phone"
		case "A2P_SMSIN", "P2P_SMSIN":
			row[col["Call Type"]] = ct
			row[col["Type"]] = "SMS"
		default:
			row[col["Call Type"]] = ct
		}
		row[col["Crime"]] = crime

		/* cell enrichment */
		firstID := cleanCGI(rec[idxFirst])
		lastID := cleanCGI(rec[idxLast])
		row[col["First Cell ID"]] = firstID
		row[col["Last Cell ID"]] = lastID
		enrich(row, col, firstID, true, "jio")
		enrich(row, col, lastID, false, "jio")

		/* ---------------- B-Party logic ---------------- */
		var bParty string
		callRaw := strings.Trim(rec[idxCalling], "'\" ")
		calledRaw := strings.Trim(rec[idxCalled], "'\" ")
		cdrDigits := normalizeMSISDN(cdr)
		callDigits := normalizeMSISDN(callRaw)
		calledDigits := normalizeMSISDN(calledRaw)

		switch {
		case callDigits == cdrDigits && calledRaw != "":
			bParty = calledRaw
		case calledDigits == cdrDigits && callRaw != "":
			bParty = callRaw
		case callDigits != "" && callDigits != cdrDigits:
			bParty = callRaw
		case calledDigits != "" && calledDigits != cdrDigits:
			bParty = calledRaw
		default:
			if callRaw != "" {
				bParty = callRaw
			} else {
				bParty = calledRaw
			}
		}
		row[col["B Party"]] = bParty

		/* ---------------- Provider / Circle / Operator via LRN ------------- */
		lrnDigits := nonDigit.ReplaceAllString(lrnRaw, "")
		if info, ok := lrnDB[lrnDigits]; ok {
			row[col["B Party Provider"]] = info.Provider
			row[col["B Party Circle"]] = info.Circle
			row[col["B Party Operator"]] = info.Operator
		}

		w.Write(row)
	}

	if len(firstRec) > 0 {
		write(firstRec)
	}
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) == 0 {
			continue
		}
		write(rec)
	}
	w.Flush()
	return out, w.Error()
}

/* ── small utility ───────────────────────────────────────────────────────── */
func colIdx(header []string, key string) int {
	key = norm(key)
	for i, h := range header {
		if norm(h) == key {
			return i
		}
	}
	return -1
}
