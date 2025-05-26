package airtel

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
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

	"github.com/xuri/excelize/v2"
)

var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Lat", "Long", "Azimuth",
	"Crime", "Circle(A-party)", "Operator(A-party)", "LRN",
	"CallForward", "B Party Provider", "B Party Circle",
	"Type", "IMEI Manufacturer", "TimeHH",
}

var (
	spaceRE  = regexp.MustCompile(`\s+`)
	nonDigit = regexp.MustCompile(`\D`)
)

func norm(s string) string   { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }
func digits(s string) string { return nonDigit.ReplaceAllString(s, "") }
func last10(s string) string {
	d := digits(s)
	if len(d) > 10 {
		return d[len(d)-10:]
	}
	if len(d) == 10 {
		return d
	}
	return ""
}

// fuzzy check: does a header look like a “B Party …” column?
func looksLikeBPartyHeader(h string) bool {
	h = norm(h)
	return strings.Contains(h, "b party") &&
		(strings.Contains(h, "no") ||
			strings.Contains(h, "number") ||
			strings.Contains(h, "mobile") ||
			strings.Contains(h, "phone"))
}

var (
	alias2canon = map[string]string{}                                      // Headers.csv mappings
	callAlias   = map[string]struct{}{}                                    // Call_types.csv
	lrnDB       = map[string]struct{ Provider, Circle, Operator string }{} // LRN.csv
	cellDB      *sql.DB                                                    // SQLite connection
)

// loadCSV reads a small CSV file into [][]string (nil on error)
func loadCSV(path string) [][]string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	rows, _ := csv.NewReader(f).ReadAll()
	return rows
}

func init() {
	// 1) Headers.csv → alias2canon
	for _, r := range loadCSV("airtel/data/Headers.csv") {
		if len(r) >= 2 {
			alias2canon[norm(r[0])] = r[1]
			alias2canon[norm(r[1])] = r[0]
		}
	}
	// 2) Call_types.csv → callAlias
	for _, r := range loadCSV("airtel/data/Call_types.csv") {
		if len(r) > 0 {
			callAlias[norm(r[0])] = struct{}{}
		}
	}
	// 3) LRN.csv → lrnDB
	if rows := loadCSV("airtel/data/LRN.csv"); len(rows) > 1 {
		h := rows[0]
		idx := func(keys ...string) int {
			for i, col := range h {
				for _, k := range keys {
					if norm(col) == norm(k) {
						return i
					}
				}
			}
			return -1
		}
		iLRN, iTSP, iCir := idx("lrn", "lrn no"), idx("tsp", "provider"), idx("circle")
		for _, r := range rows[1:] {
			if iLRN >= 0 && iLRN < len(r) {
				key := digits(r[iLRN])
				if key == "" {
					continue
				}
				prov, cir := "", ""
				if iTSP >= 0 && iTSP < len(r) {
					prov = strings.TrimSpace(r[iTSP])
				}
				if iCir >= 0 && iCir < len(r) {
					cir = strings.TrimSpace(r[iCir])
				}
				lrnDB[key] = struct{ Provider, Circle, Operator string }{prov, cir, prov}
			}
		}
	}

	// 4) open SQLite cell DB from airtel/data directory
	dbPath := filepath.Join("airtel", "data", "testnewcellids.db")
	var err error
	cellDB, err = sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", dbPath))
	if err != nil {
		panic(fmt.Errorf("cannot open cell DB at %s: %w", dbPath, err))
	}
}

// lookupCell returns address, lat, long, azimuth for a given CGI
func lookupCell(id string) (addr, lat, lon, az string, ok bool) {
	const q = `
        SELECT address, latitude, longitude, azimuth
          FROM cellids
         WHERE cellid=? OR REPLACE(cellid,'-','')=?
         LIMIT 1`
	err := cellDB.QueryRow(q, id, id).Scan(&addr, &lat, &lon, &az)
	return addr, lat, lon, az, err == nil
}

func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if norm(r.FormValue("tsp_type")) != "airtel" {
		http.Error(w, "Only Airtel", http.StatusBadRequest)
		return
	}
	crime := r.FormValue("crime_number")

	fh, hdr, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer fh.Close()

	os.MkdirAll("uploads", 0755)
	os.MkdirAll("filtered", 0755)
	up := filepath.Join("uploads", hdr.Filename)
	fout, _ := os.Create(up)
	io.Copy(fout, fh)
	fout.Close()

	book, err := processAirtel(up, crime, strings.Title(norm(r.FormValue("tsp_type"))))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	fmt.Fprintf(w, "/download/%s\n", filepath.Base(book))
}

func processAirtel(src, crime, operator string) (string, error) {
	f, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer f.Close()
	rdr := csv.NewReader(f)

	// 1) detect header + CDR + first/last positions
	var header []string
	var cdr string
	idxFirst, idxLast := -1, -1
	for {
		rec, err := rdr.Read()
		if err == io.EOF {
			return "", fmt.Errorf("no header")
		}
		if err != nil {
			continue
		}
		if cdr == "" {
			if m := regexp.MustCompile(`Mobile No '(\d+)'`).FindStringSubmatch(strings.Join(rec, " ")); len(m) > 1 {
				cdr = m[1]
			}
		}
		if len(rec) > 0 && strings.Contains(rec[0], "Target No") {
			header = rec
			for i, h := range header {
				switch norm(h) {
				case "first cgi":
					idxFirst = i
				case "last cgi":
					idxLast = i
				}
			}
			break
		}
	}
	if cdr == "" {
		return "", fmt.Errorf("CDR not found")
	}

	// 2) build src→dst map and gather possible B-Party columns
	dstIdx := make(map[string]int, len(targetHeader))
	for i, h := range targetHeader {
		dstIdx[h] = i
	}

	src2dst := map[int]int{}
	var bpartyCols []int

	for i, h := range header {
		n := norm(h)

		switch {
		case alias2canon[n] == "B Party":
			src2dst[i] = dstIdx["B Party"]
			bpartyCols = append(bpartyCols, i)

		case looksLikeBPartyHeader(h):
			src2dst[i] = dstIdx["B Party"]
			bpartyCols = append(bpartyCols, i)

		default:
			if canon, ok := alias2canon[n]; ok {
				src2dst[i] = dstIdx[canon]
			} else if _, ok := callAlias[n]; ok {
				src2dst[i] = dstIdx["Call Type"]
			} else {
				for _, th := range targetHeader {
					if norm(th) == n {
						src2dst[i] = dstIdx[th]
						break
					}
				}
			}
		}
	}

	if idxFirst >= 0 {
		src2dst[idxFirst] = dstIdx["First Cell ID"]
	}
	if idxLast >= 0 {
		src2dst[idxLast] = dstIdx["Last Cell ID"]
	}

	// buffers for sheets
	report := [][]string{targetHeader}
	type agg struct{ prov string; calls int; dur float64 }
	summaryAgg := map[string]*agg{}
	type stay struct{ addr, lat, lon, az, first, last string; total int }
	stays := map[string]*stay{}

	// per-row loop
	for {
		rec, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) == 0 {
			continue
		}

		row := make([]string, len(targetHeader))

		// 3a) copy via src2dst
		for s, d := range src2dst {
			if s >= len(rec) {
				continue
			}
			val := strings.Trim(rec[s], `"' `)
			if d == dstIdx["B Party"] { // extra sanitising
				if dig := last10(val); dig != "" && dig != cdr {
					val = dig
				} else {
					val = strings.TrimSpace(val)
				}
			}
			row[d] = val
		}

		// 3b) fixed fields
		row[dstIdx["CdrNo"]] = cdr
		row[dstIdx["Crime"]] = crime
		if t := row[dstIdx["Time"]]; len(t) >= 2 {
			row[dstIdx["TimeHH"]] = t[:2]
		}
		row[dstIdx["Operator(A-party)"]] = operator

		// strip hyphens from CGI
		firstID := strings.ReplaceAll(row[dstIdx["First Cell ID"]], "-", "")
		lastID := strings.ReplaceAll(row[dstIdx["Last Cell ID"]], "-", "")
		row[dstIdx["First Cell ID"]] = firstID
		row[dstIdx["Last Cell ID"]] = lastID

		// copy Duration, Roaming, LRN, CallForward
		copyIf := func(key string, syns ...string) {
			for _, syn := range syns {
				if idx := colIdx(header, syn); idx >= 0 && idx < len(rec) {
					row[dstIdx[key]] = strings.Trim(rec[idx], `"' `)
					return
				}
			}
		}
		copyIf("Duration", "dur(s)", "call duration")
		copyIf("Roaming", "roam nw", "roaming circle name")
		copyIf("LRN", "lrn called no", "lrn no")
		copyIf("CallForward", "call fow no", "call forwarding")

		// secondary fallback for B-Party
		if row[dstIdx["B Party"]] == "" {
			for _, idx := range bpartyCols {
				if idx < len(rec) {
					raw := strings.Trim(rec[idx], `"' `)
					if dig := last10(raw); dig != "" && dig != cdr {
						row[dstIdx["B Party"]] = dig
						break
					}
					if raw != "" && raw != cdr {
						row[dstIdx["B Party"]] = raw
						break
					}
				}
			}
		}

		// final fallback: calling / called
		if row[dstIdx["B Party"]] == "" {
			if i1, i2 := colIdx(header, "calling party telephone number"), colIdx(header, "called party telephone number"); i1 >= 0 && i2 >= 0 {
				a, b := rec[i1], rec[i2]
				switch {
				case last10(a) == cdr:
					row[dstIdx["B Party"]] = last10(b)
				case last10(b) == cdr:
					row[dstIdx["B Party"]] = last10(a)
				default:
					row[dstIdx["B Party"]] = last10(b)
				}
			}
		}

		// enrich from LRN.csv
		if info, ok := lrnDB[digits(row[dstIdx["LRN"]])]; ok {
			if row[dstIdx["B Party Provider"]] == "" {
				row[dstIdx["B Party Provider"]] = info.Provider
			}
			if row[dstIdx["B Party Circle"]] == "" {
				row[dstIdx["B Party Circle"]] = info.Circle
			}
		}

		// lookupCell
		if addr, lat, lon, az, ok := lookupCell(firstID); ok {
			if row[dstIdx["First Cell ID Address"]] == "" {
				row[dstIdx["First Cell ID Address"]] = addr
			}
			row[dstIdx["Lat"]] = lat
			row[dstIdx["Long"]] = lon
			row[dstIdx["Azimuth"]] = az
		}
		if addr2, _, _, _, ok2 := lookupCell(lastID); ok2 {
			if row[dstIdx["Last Cell ID Address"]] == "" {
				row[dstIdx["Last Cell ID Address"]] = addr2
			}
		}

		report = append(report, row)

		// summary aggregations
		bp := row[dstIdx["B Party"]]
		if bp == "" {
			bp = "(blank)"
		}
		a := summaryAgg[bp]
		if a == nil {
			a = &agg{prov: row[dstIdx["B Party Provider"]]}
			summaryAgg[bp] = a
		}
		a.calls++
		if d, e := strconv.ParseFloat(row[dstIdx["Duration"]], 64); e == nil {
			a.dur += d
		}

		// stay aggregation
		ts := row[dstIdx["Date"]] + " " + row[dstIdx["Time"]]
		s2 := stays[firstID]
		if s2 == nil {
			s2 = &stay{
				addr:  row[dstIdx["First Cell ID Address"]],
				lat:   row[dstIdx["Lat"]],
				lon:   row[dstIdx["Long"]],
				az:    row[dstIdx["Azimuth"]],
				first: ts,
				last:  ts,
				total: 1,
			}
			stays[firstID] = s2
		} else {
			s2.total++
			s2.last = ts
		}
	}

	// build summary + max* sheets (unchanged from previous versions) … ─────────────
	summary := [][]string{{"CdrNo", "B Party", "Provider", "Total Calls", "Total Duration"}}
	maxC := [][]string{{"CdrNo", "B Party", "Total Calls", "Provider"}}
	maxD := [][]string{{"CdrNo", "B Party", "Total Duration", "Provider"}}
	maxS := [][]string{{"CdrNo", "Cell ID", "Total Calls", "Address", "Lat", "Long", "Azimuth", "First", "Last"}}

	for bp, a := range summaryAgg {
		summary = append(summary, []string{cdr, bp, a.prov, strconv.Itoa(a.calls), fmt.Sprintf("%.0f", a.dur)})
	}
	type kv struct{ k string; v *agg }
	list := make([]kv, 0, len(summaryAgg))
	for k, v := range summaryAgg {
		list = append(list, kv{k, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].v.calls > list[j].v.calls })
	for _, kvp := range list {
		maxC = append(maxC, []string{cdr, kvp.k, strconv.Itoa(kvp.v.calls), kvp.v.prov})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].v.dur > list[j].v.dur })
	for _, kvp := range list {
		maxD = append(maxD, []string{cdr, kvp.k, fmt.Sprintf("%.0f", kvp.v.dur), kvp.v.prov})
	}
	for id, st := range stays {
		maxS = append(maxS, []string{cdr, id, strconv.Itoa(st.total), st.addr, st.lat, st.lon, st.az, st.first, st.last})
	}

	// write Excel
	x := excelize.NewFile()
	add := func(name string, rows [][]string) {
		idx, _ := x.NewSheet(name)
		for r, row := range rows {
			for c, v := range row {
				cell, _ := excelize.CoordinatesToCellName(c+1, r+1)
				x.SetCellStr(name, cell, v)
			}
		}
		if name == "report" {
			x.SetActiveSheet(idx)
		}
	}
	add("report", report)
	add("summary", summary)
	add("max_calls", maxC)
	add("max_duration", maxD)
	add("max_stay", maxS)
	x.DeleteSheet("Sheet1")

	out := filepath.Join("filtered", cdr+"_all_reports.xlsx")
	if err := x.SaveAs(out); err != nil {
		return "", err
	}
	return out, nil
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
