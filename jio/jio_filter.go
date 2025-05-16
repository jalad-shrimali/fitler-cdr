package jio

import (
	"embed"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sort"
	"strings"
	"time"
)

/* ── canonical 26-column header for filtered output ───────── */
var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Main City(First CellID)", "Sub City (First CellID)", "Lat-Long-Azimuth (First CellID)",
	"Crime", "Circle", "Operator", "LRN",
	"CallForward", "B Party Provider", "B Party Circle", "B Party Operator",
	"Type", "IMEI Manufacturer",
}

/* ── helpers ── */
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

/* ── banner CDR number extractor ── */
var jioCdrRE = regexp.MustCompile(`(?i)input value[^0-9]*([0-9]{8,15})`)
func extractCdrNumber(line string) string {
	if m := jioCdrRE.FindStringSubmatch(line); len(m) > 1 { return m[1] }
	return ""
}

/* ── embedded lookup data ── */
//go:embed data/*
var dataFS embed.FS

/* Cell and LRN structures */
type CellInfo struct{ Addr, Sub, Main, LatLonAz string }
type LRNInfo struct{ Provider, Circle, Operator string }

var (
	cellDB = map[string]map[string]CellInfo{}
	lrnDB  = map[string]LRNInfo{}
)

func init() {
	if err := loadCells("jio", "data/jio_cells.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(fmt.Errorf("loadCells jio failed: %w", err))
	}
	if err := loadLRN("data/LRN.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		// Just warn, LRN missing won't crash
		fmt.Printf("Warning: LRN.csv not loaded: %v\n", err)
	}
}

/* loadCells loads cell DB from CSV */
func loadCells(tsp, path string) error {
	f, err := dataFS.Open(path)
	if err != nil { return err }
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil { return err }
	col := func(keys ...string) int {
		for i, h := range header {
			for _, k := range keys {
				if norm(h) == norm(k) { return i }
			}
		}
		return -1
	}

	iID := col("cgi", "cell id", "cellid")
	iAddr := col("address")
	iSub := col("subcity", "sub city")
	iMain := col("maincity", "main city", "city")
	iLat := col("latitude", "lat")
	iLon := col("longitude", "lon", "long")
	iAz := col("azimuth", "azm", "az")

	if iID == -1 { return fmt.Errorf("no CGI column in %s", path) }
	cellDB[tsp] = map[string]CellInfo{}

	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }
		rawID := strings.TrimSpace(rec[iID])
		if rawID == "" { continue }
		info := CellInfo{
			Addr:     pick(rec, iAddr),
			Sub:      pick(rec, iSub),
			Main:     pick(rec, iMain),
			LatLonAz: buildLat(rec, iLat, iLon, iAz),
		}
		cellDB[tsp][rawID] = info
		cellDB[tsp][digits(rawID)] = info
	}
	return nil
}

/* loadLRN loads LRN DB */
func loadLRN(path string) error {
	f, err := dataFS.Open(path)
	if err != nil { return err }
	defer f.Close()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil { return err }

	idxLRN := colIdxAny(header, "lrn no", "lrn", "lrn number")
	idxTSP := colIdxAny(header, "tsp", "provider", "tsp-lsa")
	idxCircle := colIdxAny(header, "circle")
	if idxLRN == -1 || idxTSP == -1 {
		return fmt.Errorf("LRN.csv missing LRN/TSP columns")
	}

	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }

		key := digits(rec[idxLRN])
		if key == "" { continue }
		lrnDB[key] = LRNInfo{
			Provider: pick(rec, idxTSP),
			Circle:   pick(rec, idxCircle),
			Operator: pick(rec, idxTSP), // fallback operator = provider
		}
	}
	return nil
}

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

/* saveUploaded saves uploaded file */
func saveUploaded(r io.Reader, dst string) error {
	f, err := os.Create(dst)
	if err != nil { return err }
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

/* --- main handler --- */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	if strings.ToLower(r.FormValue("tsp_type")) != "jio" {
		http.Error(w, "Only Jio supported", 400)
		return
	}
	crime := r.FormValue("crime_number")

	fh, hdr, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer fh.Close()

	os.MkdirAll("uploads", 0o755)
	os.MkdirAll("filtered", 0o755)

	src := filepath.Join("uploads", hdr.Filename)
	if err := saveUploaded(fh, src); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	filtered, summary, maxCalls, maxDuration, maxStay, err := normJio(src, crime)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Fprintf(w, "/download/%s\n/download/%s\n/download/%s\n/download/%s\n/download/%s\n",
		filepath.Base(filtered), filepath.Base(summary), filepath.Base(maxCalls), filepath.Base(maxDuration), filepath.Base(maxStay))
}

/* Core normalization + summaries + max reports */
func normJio(src, crime string) (string, string, string, string, string, error) {
	in, err := os.Open(src)
	if err != nil { return "", "", "", "", "", err }
	defer in.Close()
	r := csv.NewReader(in)

	/* 1. Find header and CDR */
	var header []string
	var cdr string
	var iFirst, iLast, iCalling, iCalled, iInput int = -1, -1, -1, -1, -1
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return "", "", "", "", "", errors.New("no header found")
		}
		if err != nil { continue }
		if cdr == "" {
			cdr = extractCdrNumber(strings.Join(rec, " "))
		}
		for i, h := range rec {
			switch norm(h) {
			case "first cgi", "first cell id":
				iFirst = i
			case "last cgi", "last cell id":
				iLast = i
			case "calling party telephone number":
				iCalling = i
			case "called party telephone number":
				iCalled = i
			}
			if strings.Contains(strings.ToLower(h), "input value") {
				iInput = i
			}
		}
		if iFirst != -1 && iLast != -1 {
			header = rec
			break
		}
	}
	var firstRec []string
	if cdr == "" && iInput != -1 {
		firstRec, _ = r.Read()
		if len(firstRec) > iInput {
			if m := regexp.MustCompile(`\d{8,15}`).FindString(firstRec[iInput]); m != "" {
				cdr = m
			}
		}
	}
	if cdr == "" {
		return "", "", "", "", "", errors.New("CDR not found")
	}
	cdr10 := last10(cdr)

	/* Setup filtered report */
	filteredPath := filepath.Join("filtered", cdr+"_reports.csv")
	fout, _ := os.Create(filteredPath)
	defer fout.Close()
	fw := csv.NewWriter(fout)
	_ = fw.Write(targetHeader)
	col := map[string]int{}
	for i, h := range targetHeader { col[h] = i }
	blank := make([]string, len(targetHeader))

	/* Summary map: key = B Party */
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
		if _, e := time.Parse(timeLayout, dt); e == nil {
			return dt
		}
		return dt
	}

	/* Max stay: keyed by cell ID */
	type maxStayAgg struct {
		CellID, Addr, Lat, Lon, Azimuth, Roaming, FirstCall, LastCall string
		TotalCalls                                                   int
	}
	maxStay := map[string]*maxStayAgg{}

	/* Copy helper */
	cp := func(rec []string, src int, dst string, row []string) {
		if src >= 0 && src < len(rec) {
			row[col[dst]] = strings.Trim(rec[src], "'\" ")
		}
	}

	/* Write one filtered row and update summaries */
	writeRow := func(rec []string) {
		if len(rec) == 0 {
			return
		}
		row := append([]string(nil), blank...)
		row[col["CdrNo"]] = cdr

		// Basic copies
		cp(rec, colIdx(header, "call date"), "Date", row)
		cp(rec, colIdx(header, "call time"), "Time", row)
		cp(rec, colIdxAny(header, "dur(s)", "duration(sec)", "call duration"), "Duration", row)
		cp(rec, colIdx(header, "imei"), "IMEI", row)
		cp(rec, colIdx(header, "imsi"), "IMSI", row)
		cp(rec, colIdxAny(header, "lrn called no", "lrn no", "lrn"), "LRN", row)
		cp(rec, colIdxAny(header, "call forward", "call fwd no", "call fow no"), "CallForward", row)
		cp(rec, colIdx(header, "roaming circle name"), "Roaming", row)

		// Call Type logic
		ctIdx := colIdx(header, "call type")
		ct := ""
		if ctIdx >= 0 && ctIdx < len(rec) {
			ct = strings.ToUpper(strings.Trim(rec[ctIdx], "'\" "))
		}
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

		// First and Last Cell IDs
		firstID := cleanCGI(rec[iFirst])
		lastID := cleanCGI(rec[iLast])
		row[col["First Cell ID"]] = firstID
		row[col["Last Cell ID"]] = lastID
		enrich(row, col, firstID, true)
		enrich(row, col, lastID, false)

		// B Party logic
		callRaw := strings.Trim(rec[iCalling], "'\" ")
		calledRaw := strings.Trim(rec[iCalled], "'\" ")
		callDigits := last10(callRaw)
		calledDigits := last10(calledRaw)

		switch {
		case callDigits == cdr10 && calledRaw != "":
			row[col["B Party"]] = calledRaw
		case calledDigits == cdr10 && callRaw != "":
			row[col["B Party"]] = callRaw
		default:
			if calledRaw != "" {
				row[col["B Party"]] = calledRaw
			} else {
				row[col["B Party"]] = callRaw
			}
		}
		bKey := row[col["B Party"]]
		if bKey == "" {
			bKey = "(blank)"
		}

		// Provider info via LRN
		lrnDigits := digits(row[col["LRN"]])
		if info, ok := lrnDB[lrnDigits]; ok {
			row[col["B Party Provider"]] = info.Provider
			row[col["B Party Circle"]] = info.Circle
			row[col["B Party Operator"]] = info.Operator
		} else {
			// fallback: if blank, fill as Unknown
			if row[col["B Party Provider"]] == "" {
				row[col["B Party Provider"]] = "Unknown"
			}
		}

		// Write filtered row
		fw.Write(row)

		// Update summary aggregator
		a, ok := summary[bKey]
		if !ok {
			a = &agg{
				BParty: bKey,
				SDR: row[col["B Party Operator"]],
				Provider: row[col["B Party Provider"]],
				Type: row[col["Type"]],
				Days: make(map[string]struct{}),
				CellIds: make(map[string]struct{}),
				Imeis: make(map[string]struct{}),
				Imsis: make(map[string]struct{}),
			}
			summary[bKey] = a
		}

		a.TotalCalls++
		switch row[col["Call Type"]] {
		case "CALL_OUT": a.OutCalls++
		case "CALL_IN": a.InCalls++
		default:
			if strings.Contains(row[col["Call Type"]], "SMS") {
				if strings.HasSuffix(row[col["Call Type"]], "OUT") {
					a.OutSMS++
				} else {
					a.InSMS++
				}
			} else {
				a.OtherCalls++
			}
		}
		if row[col["Roaming"]] != "" {
			if strings.Contains(row[col["Call Type"]], "SMS") {
				a.RoamSMS++
			} else {
				a.RoamCalls++
			}
		}

		if dur, e := strconv.ParseFloat(row[col["Duration"]], 64); e == nil {
			a.TotalDuration += dur
		}

		a.Days[row[col["Date"]]] = struct{}{}
		if firstID != "" {
			a.CellIds[firstID] = struct{}{}
		}
		if lastID != "" {
			a.CellIds[lastID] = struct{}{}
		}
		if v := row[col["IMEI"]]; v != "" {
			a.Imeis[v] = struct{}{}
		}
		if v := row[col["IMSI"]]; v != "" {
			a.Imsis[v] = struct{}{}
		}

		dt := parseDT(row[col["Date"]], row[col["Time"]])
		if a.FirstCall == "" || dt < a.FirstCall {
			a.FirstCall = dt
		}
		if a.LastCall == "" || dt > a.LastCall {
			a.LastCall = dt
		}

		// Update maxStay aggregator for first cell
		if firstID != "" {
			ms, ok := maxStay[firstID]
			if !ok {
				ms = &maxStayAgg{
					CellID: firstID,
					Addr:   row[col["First Cell ID Address"]],
					Lat:    "",
					Lon:    "",
					Azimuth: "",
					Roaming: row[col["Roaming"]],
					FirstCall: dt,
					LastCall:  dt,
					TotalCalls: 1,
				}
				// parse lat/lon/azimuth
				if llaz := row[col["Lat-Long-Azimuth (First CellID)"]]; llaz != "" {
					parts := strings.Split(llaz, ",")
					if len(parts) >= 2 {
						ms.Lat = strings.TrimSpace(parts[0])
						ms.Lon = strings.TrimSpace(parts[1])
					}
					if len(parts) == 3 {
						ms.Azimuth = strings.TrimSpace(parts[2])
					}
				}
				maxStay[firstID] = ms
			} else {
				ms.TotalCalls++
				if dt < ms.FirstCall { ms.FirstCall = dt }
				if dt > ms.LastCall { ms.LastCall = dt }
			}
		}
	}

	if len(firstRec) > 0 {
		writeRow(firstRec)
	}
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
	fw.Flush()

	// Write multi-party summary
	summaryPath := filepath.Join("filtered", cdr+"_summary_reports.csv")
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
			strconv.Itoa(a.TotalCalls), strconv.Itoa(a.OutCalls), strconv.Itoa(a.InCalls),
			strconv.Itoa(a.OutSMS), strconv.Itoa(a.InSMS), strconv.Itoa(a.OtherCalls),
			strconv.Itoa(a.RoamCalls), strconv.Itoa(a.RoamSMS),
			fmt.Sprintf("%.0f", a.TotalDuration),
			strconv.Itoa(len(a.Days)), strconv.Itoa(len(a.CellIds)),
			strconv.Itoa(len(a.Imeis)), strconv.Itoa(len(a.Imsis)),
			a.FirstCall, a.LastCall,
		})
	}
	sw.Flush()

	// Write max calls report
	maxCallsPath := filepath.Join("filtered", cdr+"_max_calls_reports.csv")
	mcF, _ := os.Create(maxCallsPath)
	defer mcF.Close()
	mcw := csv.NewWriter(mcF)

	mcw.Write([]string{"CdrNo", "B Party", "B Party SDR", "Total Calls", "Provider"})

	// Also compute total calls across all parties for summary row
	totalCalls := 0
	for _, a := range summary {
		totalCalls += a.TotalCalls
	}
	// Write total row with B Party as CDR (like your sample)
	mcw.Write([]string{"Total", cdr, "", strconv.Itoa(totalCalls), ""})

	// Sort by total calls desc (optional)
	type kv struct {
		Key string
		Val *agg
	}
	var sorted []kv
	for k, v := range summary {
		sorted = append(sorted, kv{k, v})
	}
	// Sort descending
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Val.TotalCalls > sorted[j].Val.TotalCalls })

	for _, kvp := range sorted {
		provider := kvp.Val.Provider
		if provider == "" {
			provider = "Unknown"
		}
		mcw.Write([]string{cdr, kvp.Key, "", strconv.Itoa(kvp.Val.TotalCalls), provider})
	}
	mcw.Flush()

	// Write max duration report
	maxDurationPath := filepath.Join("filtered", cdr+"_max_duration_reports.csv")
	mdF, _ := os.Create(maxDurationPath)
	defer mdF.Close()
	mdw := csv.NewWriter(mdF)

	mdw.Write([]string{"CdrNo", "B Party", "B Party SDR", "Total Duration", "Provider"})

	// Sort by total duration desc
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Val.TotalDuration > sorted[j].Val.TotalDuration })

	for _, kvp := range sorted {
		provider := kvp.Val.Provider
		if provider == "" {
			provider = "Unknown"
		}
		mdw.Write([]string{
			cdr, kvp.Key, "", fmt.Sprintf("%.0f", kvp.Val.TotalDuration), provider,
		})
	}
	mdw.Flush()

	// Write max stay report
	maxStayPath := filepath.Join("filtered", cdr+"_max_stay_reports.csv")
	msF, _ := os.Create(maxStayPath)
	defer msF.Close()
	msw := csv.NewWriter(msF)
	msw.Write([]string{
		"CdrNo", "Cell ID", "Total Calls", "Tower Address", "Latitude", "Longitude", "Azimuth", "Roaming", "First Call", "Last Call",
	})

	for _, ms := range maxStay {
		addr := ms.Addr
		if addr == "" {
			addr = "Unknown"
		}
		roaming := ms.Roaming
		if roaming == "" {
			roaming = "Unknown"
		}
		lat := ms.Lat
		if lat == "" {
			lat = "0"
		}
		lon := ms.Lon
		if lon == "" {
			lon = "0"
		}
		az := ms.Azimuth
		if az == "" {
			az = "0"
		}
		msw.Write([]string{
			cdr, ms.CellID, strconv.Itoa(ms.TotalCalls), addr, lat, lon, az, roaming, ms.FirstCall, ms.LastCall,
		})
	}
	msw.Flush()

	return filteredPath, summaryPath, maxCallsPath, maxDurationPath, maxStayPath, nil
}

/* enrich cell address fields */
func enrich(row []string, col map[string]int, id string, first bool) {
	if info, ok := findCell("jio", id); ok {
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

