package airtel

import (
	"embed"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

/* ────────── canonical 26-column layout ────────── */
var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Main City(First CellID)", "Sub City (First CellID)", "Lat-Long-Azimuth (First CellID)",
	"Crime", "Circle", "Operator", "LRN",
	"CallForward", "B Party Provider", "B Party Circle", "B Party Operator",
	"Type", "IMEI Manufacturer",
}

/* column synonyms */
var synonyms = map[string]string{
	"b party no":                  "B Party",
	"called party telephone number": "B Party",
	"date":                       "Date",
	"call date":                  "Date",
	"time":                       "Time",
	"call time":                  "Time",
	"dur(s)":                     "Duration",
	"call duration":              "Duration",
	"call type":                  "Call Type",
	"imei":                      "IMEI",
	"imsi":                      "IMSI",
	"roam nw":                   "Roaming",
	"roaming circle name":       "Circle",
	"circle":                    "Circle",
	"operator":                  "Operator",
	"lrn":                       "LRN",
	"lrn called no":             "LRN",
	"call fow no":               "CallForward",
	"call forwarding":           "CallForward",
	"lrn tsp-lsa":               "B Party Provider",
	"b party provider":          "B Party Provider",
	"b party circle":            "B Party Circle",
	"b party operator":          "B Party Operator",
	"service type":              "Type",
	"crime":                    "Crime",
}

/* helpers */
var spaceRE = regexp.MustCompile(`\s+`)
func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }

/* embedded data */
//go:embed data/*
var dataFS embed.FS

/* Cell and LRN info */
type CellInfo struct {
	Address, SubCity, MainCity, LatLongAzimuth string
}
type LRNInfo struct {
	Provider, Circle, Operator string
}

var (
	cellDB = map[string]CellInfo{}
	lrnDB  = map[string]LRNInfo{}
)

func init() {
	// Load cell DB
	cf, err := dataFS.Open("data/airtel_cells.csv")
	if err != nil {
		panic(fmt.Errorf("missing airtel_cells.csv: %w", err))
	}
	loadCells(cf)

	// Load LRN DB
	lf, err := dataFS.Open("data/LRN.csv")
	if err == nil {
		loadLRN(lf)
	}
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
		op := ""
		if opIdx, ok := h["operator"]; ok {
			op = rec[opIdx]
		}
		lrnDB[key] = LRNInfo{
			Provider: rec[h["tsp"]],
			Circle:   rec[h["circle"]],
			Operator: op,
		}
	}
}

func indexMap(header []string) map[string]int {
	m := make(map[string]int)
	for i, h := range header {
		m[norm(h)] = i
	}
	return m
}

func saveUploaded(r io.Reader, dst string) error {
	f, err := os.Create(dst)
	if err != nil { return err }
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

/* HTTP handler */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	tsp := strings.ToLower(r.FormValue("tsp_type"))
	if tsp != "airtel" {
		http.Error(w, "Only Airtel supported", 400)
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

	filtered, summary, maxCalls, maxDuration, maxStay, err := normalizeAirtel(src, crime)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Fprintf(w, "/download/%s\n/download/%s\n/download/%s\n/download/%s\n/download/%s\n",
		filepath.Base(filtered), filepath.Base(summary), filepath.Base(maxCalls), filepath.Base(maxDuration), filepath.Base(maxStay))
}

/* enrich cell info */
func enrichWithCell(row []string, col map[string]int, id string, first bool) {
	info, ok := cellDB[id]
	if !ok {
		return
	}
	if first {
		row[col["First Cell ID Address"]] = info.Address
		row[col["Sub City (First CellID)"]] = info.SubCity
		row[col["Main City(First CellID)"]] = info.MainCity
		row[col["Lat-Long-Azimuth (First CellID)"]] = info.LatLongAzimuth
	} else {
		row[col["Last Cell ID Address"]] = info.Address
	}
}

/* enrich LRN info */
func enrichWithLRN(row []string, col map[string]int) {
	lrn := strings.TrimSpace(row[col["LRN"]])
	if lrn == "" {
		return
	}
	info, ok := lrnDB[lrn]
	if !ok {
		return
	}
	if row[col["B Party Provider"]] == "" {
		row[col["B Party Provider"]] = info.Provider
	}
	row[col["B Party Circle"]] = info.Circle
	if info.Operator != "" {
		row[col["B Party Operator"]] = info.Operator
	} else {
		row[col["B Party Operator"]] = info.Provider
	}
}

func normalizeAirtel(src, crime string) (string, string, string, string, string, error) {
	in, err := os.Open(src)
	if err != nil { return "", "", "", "", "", err }
	defer in.Close()
	r := csv.NewReader(in)

	// Read header and cdr number
	var header []string
	var cdrNumber string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return "", "", "", "", "", fmt.Errorf("no header found")
		}
		if err != nil { continue }
		if cdrNumber == "" && len(rec) > 0 {
			cdrNumber = extractCdrNumber("airtel", rec[0])
		}
		if len(rec) > 0 && strings.Contains(rec[0], "Target No") {
			header = rec
			break
		}
	}
	if cdrNumber == "" {
		return "", "", "", "", "", fmt.Errorf("could not extract CDR number")
	}

	srcToDst := map[int]int{}
	col := map[string]int{}
	for i, h := range targetHeader { col[h] = i }

	firstCGI, lastCGI := -1, -1
	for i, h := range header {
		hNorm := norm(h)
		if hNorm == "first cgi" { firstCGI = i }
		if hNorm == "last cgi" { lastCGI = i }
		if canonical, ok := synonyms[hNorm]; ok {
			srcToDst[i] = col[canonical]
		}
	}
	if firstCGI == -1 || lastCGI == -1 {
		return "", "", "", "", "", fmt.Errorf("missing first/last CGI columns")
	}
	srcToDst[firstCGI] = col["First Cell ID"]
	srcToDst[lastCGI] = col["Last Cell ID"]

	filteredPath := filepath.Join("filtered", fmt.Sprintf("%s_reports.csv", cdrNumber))
	out, err := os.Create(filteredPath)
	if err != nil { return "", "", "", "", "", err }
	defer out.Close()
	w := csv.NewWriter(out)
	_ = w.Write(targetHeader)
	blank := make([]string, len(targetHeader))

	// Aggregation structs
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

	type maxStayAgg struct {
		CellID, Addr, Lat, Lon, Azimuth, Roaming, FirstCall, LastCall string
		TotalCalls                                                    int
	}
	maxStay := map[string]*maxStayAgg{}

	timeLayout := "2006-01-02 15:04:05"
	parseDT := func(d, t string) string {
		dt := strings.TrimSpace(d) + " " + strings.TrimSpace(t)
		if _, e := time.Parse(timeLayout, dt); e == nil {
			return dt
		}
		return dt
	}

	writeRow := func(rec []string) {
		if len(rec) == 0 { return }
		row := append([]string(nil), blank...)
		row[col["CdrNo"]] = cdrNumber
		row[col["Crime"]] = crime

		for s, d := range srcToDst {
			if s < len(rec) {
				val := strings.Trim(rec[s], "'\" ")
				if targetHeader[d] == "Call Type" {
					// normalize call types
					switch strings.ToUpper(val) {
					case "IN", "A_IN": val = "CALL_IN"
					case "OUT", "A_OUT": val = "CALL_OUT"
					}
				}
				if targetHeader[d] == "Type" {
					if strings.EqualFold(val, "pre") { val = "Prepaid" }
					if strings.EqualFold(val, "post") { val = "Postpaid" }
				}
				row[d] = val
			}
		}

		// Ensure clean CGI fields
		if first := cleanCGI(rec[firstCGI]); first != "" {
			row[col["First Cell ID"]] = first
		}
		if last := cleanCGI(rec[lastCGI]); last != "" {
			row[col["Last Cell ID"]] = last
		}

		enrichWithCell(row, col, row[col["First Cell ID"]], true)
		enrichWithCell(row, col, row[col["Last Cell ID"]], false)
		enrichWithLRN(row, col)

		w.Write(row)

		bKey := row[col["B Party"]]
		if bKey == "" { bKey = "(blank)" }
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
		case "CALL_IN": a.InCalls++
		default:
			if strings.Contains(row[col["Call Type"]], "SMS") {
				if strings.HasSuffix(row[col["Call Type"]], "OUT") { a.OutSMS++ } else { a.InSMS++ }
			} else { a.OtherCalls++ }
		}
		if row[col["Roaming"]] != "" {
			if strings.Contains(row[col["Call Type"]], "SMS") { a.RoamSMS++ } else { a.RoamCalls++ }
		}
		if dur, err := strconv.ParseFloat(row[col["Duration"]], 64); err == nil {
			a.TotalDuration += dur
		}

		a.Days[row[col["Date"]]] = struct{}{}
		if firstID := row[col["First Cell ID"]]; firstID != "" { a.CellIds[firstID] = struct{}{} }
		if lastID := row[col["Last Cell ID"]]; lastID != "" { a.CellIds[lastID] = struct{}{} }
		if v := row[col["IMEI"]]; v != "" { a.Imeis[v] = struct{}{} }
		if v := row[col["IMSI"]]; v != "" { a.Imsis[v] = struct{}{} }

		dt := parseDT(row[col["Date"]], row[col["Time"]])
		if a.FirstCall == "" || dt < a.FirstCall { a.FirstCall = dt }
		if a.LastCall == "" || dt > a.LastCall { a.LastCall = dt }

		if firstID := row[col["First Cell ID"]]; firstID != "" {
			ms, ok := maxStay[firstID]
			if !ok {
				ms = &maxStayAgg{
					CellID:    firstID,
					Addr:      row[col["First Cell ID Address"]],
					Roaming:   row[col["Roaming"]],
					FirstCall: dt,
					LastCall:  dt,
					TotalCalls: 1,
				}
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

	// Write remaining rows
	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }
		writeRow(rec)
	}
	w.Flush()

	// Write summary report
	summaryPath := filepath.Join("filtered", cdrNumber+"_summary_reports.csv")
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
			cdrNumber, a.BParty, a.SDR, a.Provider, a.Type,
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

	// Max calls report
	maxCallsPath := filepath.Join("filtered", cdrNumber+"_max_calls_reports.csv")
	mcF, _ := os.Create(maxCallsPath)
	defer mcF.Close()
	mcw := csv.NewWriter(mcF)
	mcw.Write([]string{"CdrNo", "B Party", "B Party SDR", "Total Calls", "Provider"})

	totalCalls := 0
	for _, a := range summary {
		totalCalls += a.TotalCalls
	}
	mcw.Write([]string{"Total", cdrNumber, "", strconv.Itoa(totalCalls), ""})

	type kv struct {
		Key string
		Val *agg
	}
	var sorted []kv
	for k, v := range summary {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Val.TotalCalls > sorted[j].Val.TotalCalls })

	for _, kvp := range sorted {
		provider := kvp.Val.Provider
		if provider == "" { provider = "Unknown" }
		mcw.Write([]string{cdrNumber, kvp.Key, "", strconv.Itoa(kvp.Val.TotalCalls), provider})
	}
	mcw.Flush()

	// Max duration report
	maxDurationPath := filepath.Join("filtered", cdrNumber+"_max_duration_reports.csv")
	mdF, _ := os.Create(maxDurationPath)
	defer mdF.Close()
	mdw := csv.NewWriter(mdF)
	mdw.Write([]string{"CdrNo", "B Party", "B Party SDR", "Total Duration", "Provider"})

	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Val.TotalDuration > sorted[j].Val.TotalDuration })

	for _, kvp := range sorted {
		provider := kvp.Val.Provider
		if provider == "" { provider = "Unknown" }
		mdw.Write([]string{
			cdrNumber, kvp.Key, "", fmt.Sprintf("%.0f", kvp.Val.TotalDuration), provider,
		})
	}
	mdw.Flush()

	// Max stay report
	maxStayPath := filepath.Join("filtered", cdrNumber+"_max_stay_reports.csv")
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
			cdrNumber, ms.CellID, strconv.Itoa(ms.TotalCalls), addr, lat, lon, az, roaming, ms.FirstCall, ms.LastCall,
		})
	}
	msw.Flush()

	return filteredPath, summaryPath, maxCallsPath, maxDurationPath, maxStayPath, nil
}

func extractCdrNumber(tsp, content string) string {
	switch strings.ToLower(tsp) {
	case "airtel":
		re := regexp.MustCompile(`Mobile No '(\d+)'`)
		if m := re.FindStringSubmatch(content); len(m) > 1 {
			return m[1]
		}
	case "jio":
		re := regexp.MustCompile(`Input Value : (\d+)`)
		if m := re.FindStringSubmatch(content); len(m) > 1 {
			return m[1]
		}
	case "vi":
		re := regexp.MustCompile(`MSISDN : - (\d+)`)
		if m := re.FindStringSubmatch(content); len(m) > 1 {
			return m[1]
		}
	}
	return ""
}

func cleanCGI(raw string) string {
	return strings.ReplaceAll(raw, "-", "")
}
