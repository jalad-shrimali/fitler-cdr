package vi

import (
	"embed"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"net/http"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

/* canonical 26-column output header */
var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Main City(First CellID)", "Sub City (First CellID)", "Lat-Long-Azimuth (First CellID)",
	"Crime", "Circle", "Operator", "LRN",
	"CallForward", "B Party Provider", "B Party Circle", "B Party Operator",
	"Type", "IMEI Manufacturer",
}

/* helpers */
var (
	spaceRE  = regexp.MustCompile(`\s+`)
	nonDigit = regexp.MustCompile(`\D`)
)
func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }
func digits(s string) string { return nonDigit.ReplaceAllString(s, "") }
func cleanCGI(s string) string { return digits(s) }

/* column index finder */
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

/* CDR extractor */
var msisdnRE = regexp.MustCompile(`(?i)msisdn[^0-9]*([0-9]{8,15})`)
func extractCdrNumber(line string) string {
	if m := msisdnRE.FindStringSubmatch(line); len(m) > 1 {
		return m[1]
	}
	return ""
}

/* embedded data */
//go:embed data/*
var dataFS embed.FS

/* Cell and LRN types */
type CellInfo struct{ Addr, Sub, Main, LatLonAz string }
type LRNInfo struct{ Provider, Circle, Operator string }

var (
	cellDB = map[string]map[string]CellInfo{}
	lrnDB  = map[string]LRNInfo{}
)

func init() {
	if err := loadCells("vi", "data/vi_cells.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(fmt.Errorf("loadCells vi failed: %w", err))
	}
	if err := loadLRN("data/LRN.csv"); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Printf("Warning: LRN.csv not loaded: %v\n", err)
	}
}

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
	iID := col("cgi", "cell global id", "cell id")
	iAddr := col("address", "bts location")
	iSub := col("subcity", "sub city")
	iMain := col("maincity", "city", "main city")
	iLat := col("latitude", "lat")
	iLon := col("longitude", "lon", "long")
	iAz := col("azimuth", "azm", "az")
	if iID == -1 { return fmt.Errorf("no CGI column in %s", path) }
	cellDB[tsp] = map[string]CellInfo{}
	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }
		cgi := strings.TrimSpace(rec[iID])
		if cgi == "" { continue }
		info := CellInfo{
			Addr:     pick(rec, iAddr),
			Sub:      pick(rec, iSub),
			Main:     pick(rec, iMain),
			LatLonAz: buildLat(rec, iLat, iLon, iAz),
		}
		cellDB[tsp][cgi] = info
		cellDB[tsp][digits(cgi)] = info
	}
	return nil
}

func loadLRN(path string) error {
	f, err := dataFS.Open(path)
	if err != nil { return err }
	defer f.Close()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil { return err }
	iLRN := colIdxAny(header, "lrn no", "lrn", "lrn number")
	iTSP := colIdxAny(header, "tsp", "provider", "tsp-lsa")
	iCircle := colIdxAny(header, "circle")
	if iLRN == -1 { return fmt.Errorf("no LRN column in %s", path) }
	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }
		lrn := digits(rec[iLRN])
		if lrn == "" { continue }
		lrnDB[lrn] = LRNInfo{
			Provider: pick(rec, iTSP),
			Circle:   pick(rec, iCircle),
			Operator: pick(rec, iTSP),
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

func saveUploaded(r io.Reader, dst string) error {
	f, err := os.Create(dst)
	if err != nil { return err }
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", 405)
		return
	}
	if strings.ToLower(r.FormValue("tsp_type")) != "vi" {
		http.Error(w, "Only VI supported", 400)
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

	filtered, summary, maxCalls, maxDuration, maxStay, err := normVI(src, crime)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Fprintf(w, "/download/%s\n/download/%s\n/download/%s\n/download/%s\n/download/%s\n",
		filepath.Base(filtered), filepath.Base(summary), filepath.Base(maxCalls), filepath.Base(maxDuration), filepath.Base(maxStay))
}

func last10(s string) string {
	if len(s) <= 10 {
		return s
	}
	return s[len(s)-10:]
}

func normVI(src, crime string) (string, string, string, string, string, error) {
	in, err := os.Open(src)
	if err != nil { return "", "", "", "", "", err }
	defer in.Close()
	r := csv.NewReader(in)

	// Find header and CDR
	var header []string
	var cdr string
	for {
		rec, err := r.Read()
		if err == io.EOF { return "", "", "", "", "", errors.New("no header found") }
		if err != nil { continue }
		if cdr == "" {
			cdr = extractCdrNumber(strings.Join(rec, " "))
		}
		if colIdx(rec, "call date") != -1 {
			header = rec
			break
		}
	}
	idxMSISDN := colIdxAny(header, "msisdn", "msisdn no", "msisdn number")
	firstData, err := r.Read()
	if err != nil { return "", "", "", "", "", errors.New("header present but no data") }
	if cdr == "" && idxMSISDN != -1 && idxMSISDN < len(firstData) {
		cdr = digits(firstData[idxMSISDN])
	}
	// Removed unused variable cdr10

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

	filteredPath := filepath.Join("filtered", cdr+"_reports.csv")
	fout, _ := os.Create(filteredPath)
	defer fout.Close()
	fw := csv.NewWriter(fout)
	_ = fw.Write(targetHeader)
	col := map[string]int{}
	for i, h := range targetHeader { col[h] = i }
	blank := make([]string, len(targetHeader))

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

	cp := func(rec []string, src int, dst string, row []string) {
		if src >= 0 && src < len(rec) {
			row[col[dst]] = strings.Trim(rec[src], "'\" ")
		}
	}

	writeRow := func(rec []string) {
		if len(rec) == 0 { return }
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

		// enrich cell details
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

		// Provider/circle/operator from LRN
		if l := digits(pick(rec, idxLRN)); l != "" {
			if info, ok := lrnDB[l]; ok {
				row[col["B Party Provider"]] = info.Provider
				row[col["B Party Circle"]] = info.Circle
				row[col["B Party Operator"]] = info.Operator
			}
		}

		fw.Write(row)

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

		// max stay aggregator for first cell
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

	// write all rows
	writeRow(firstData)
	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }
		writeRow(rec)
	}
	fw.Flush()

	// Write summary CSV
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

	// max calls report
	maxCallsPath := filepath.Join("filtered", cdr+"_max_calls_reports.csv")
	mcF, _ := os.Create(maxCallsPath)
	defer mcF.Close()
	mcw := csv.NewWriter(mcF)
	mcw.Write([]string{"CdrNo", "B Party", "B Party SDR", "Total Calls", "Provider"})

	totalCalls := 0
	for _, a := range summary {
		totalCalls += a.TotalCalls
	}
	mcw.Write([]string{"Total", cdr, "", strconv.Itoa(totalCalls), ""})

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
		mcw.Write([]string{cdr, kvp.Key, "", strconv.Itoa(kvp.Val.TotalCalls), provider})
	}
	mcw.Flush()

	// max duration report
	maxDurationPath := filepath.Join("filtered", cdr+"_max_duration_reports.csv")
	mdF, _ := os.Create(maxDurationPath)
	defer mdF.Close()
	mdw := csv.NewWriter(mdF)
	mdw.Write([]string{"CdrNo", "B Party", "B Party SDR", "Total Duration", "Provider"})

	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Val.TotalDuration > sorted[j].Val.TotalDuration })

	for _, kvp := range sorted {
		provider := kvp.Val.Provider
		if provider == "" { provider = "Unknown" }
		mdw.Write([]string{
			cdr, kvp.Key, "", fmt.Sprintf("%.0f", kvp.Val.TotalDuration), provider,
		})
	}
	mdw.Flush()

	// max stay report
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
