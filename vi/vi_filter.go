// vi_filter.go  (place in vi/)
package vi

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

/* ── canonical 27-column header ─────────────────────────── */
var targetHeader = []string{
	"CdrNo", "A Party", // new column
	"B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Lat", "Long", "Azimuth",
	"Crime", "Circle(A-party)", "Operator(A-party)", "LRN",
	"CallForward", "B Party Provider", "B Party Circle",
	"Type", "IMEI Manufacturer", "TimeHH",
}

/* ── helpers ── */
var (
	spaceRE  = regexp.MustCompile(`\s+`)
	nonDigit = regexp.MustCompile(`\D`)
)
func norm(s string) string   { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }
func digits(s string) string { return nonDigit.ReplaceAllString(s, "") }
func last10(s string) string { d := digits(s); if len(d) > 10 { return d[len(d)-10:] }; return d }

/* banner CDR extractor */
var msisdnRE = regexp.MustCompile(`(?i)msisdn[^0-9]*([0-9]{8,15})`)
func extractCDR(line string) string {
	if m := msisdnRE.FindStringSubmatch(line); len(m) > 1 { return m[1] }
	return ""
}
func looksLikeHeader(rec []string) bool {
	for _, h := range rec {
		if norm(h) == "call date" || norm(h) == "date" { return true }
	}
	return false
}

/* ── metadata / look-ups ── */
var (
	alias2canon = map[string]string{}
	callAlias   = map[string]struct{}{}
	lrnDB       = map[string]struct{ Provider, Circle, Operator string }{}
	cellDB      *sql.DB
)

/* load small CSVs */
func loadCSV(p string) [][]string {
	f, e := os.Open(p); if e != nil { return nil }
	defer f.Close()
	rows, _ := csv.NewReader(f).ReadAll()
	return rows
}
func init() {
	/* Headers.csv */
	for _, r := range loadCSV(filepath.Join("vi", "data", "Headers.csv")) {
		if len(r) >= 2 {
			alias2canon[norm(r[0])] = r[1]
			alias2canon[norm(r[1])] = r[0]
		}
	}
	/* Call_types.csv */
	for _, r := range loadCSV(filepath.Join("vi", "data", "Call_types.csv")) {
		if len(r) > 0 { callAlias[norm(r[0])] = struct{}{} }
	}
	/* LRN.csv */
	if rows := loadCSV(filepath.Join("vi", "data", "LRN.csv")); len(rows) > 1 {
		h := rows[0]
		idx := func(keys ...string) int {
			for i, c := range h { for _, k := range keys {
				if norm(c) == norm(k) { return i } } }
			return -1
		}
		iLRN, iTSP, iCir := idx("lrn", "lrn no"), idx("tsp", "provider"), idx("circle")
		for _, r := range rows[1:] {
			k := digits(r[iLRN]); if k == "" { continue }
			lrnDB[k] = struct{ Provider, Circle, Operator string }{
				strings.TrimSpace(r[iTSP]), strings.TrimSpace(r[iCir]), strings.TrimSpace(r[iTSP]),
			}
		}
	}
	/* SQLite towers */
	var err error
	cellDB, err = sql.Open("sqlite3",
		fmt.Sprintf("file:%s?mode=ro", filepath.Join("vi", "data", "testnewcellids.db")))
	if err != nil { panic(err) }
}
func lookupCell(id string) (addr, lat, lon, az string, ok bool) {
	const q = `SELECT address,latitude,longitude,azimuth
	           FROM cellids WHERE cellid=? OR REPLACE(cellid,'-','')=? LIMIT 1`
	err := cellDB.QueryRow(q, id, id).Scan(&addr, &lat, &lon, &az)
	return addr, lat, lon, az, err == nil
}

/* ── HTTP handler ── */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "POST only", 405); return }
	if norm(r.FormValue("tsp_type")) != "vi" { http.Error(w, "Only VI", 400); return }

	crime := r.FormValue("crime_number")

	fh, hdr, err := r.FormFile("file")
	if err != nil { http.Error(w, err.Error(), 400); return }
	defer fh.Close()

	_ = os.MkdirAll("uploads", 0o755)
	_ = os.MkdirAll("filtered", 0o755)

	src := filepath.Join("uploads", hdr.Filename)
	if err := saveFile(fh, src); err != nil {
		http.Error(w, err.Error(), 500); return
	}
	out, err := processVI(src, crime)
	if err != nil { http.Error(w, err.Error(), 500); return }
	fmt.Fprintf(w, "/download/%s\n", filepath.Base(out))
}
func saveFile(r io.Reader, dst string) error {
	f, err := os.Create(dst); if err != nil { return err }
	defer f.Close(); _, err = io.Copy(f, r); return err
}

/* ── main normaliser ───────────────────────────────────── */
func processVI(src, crime string) (string, error) {
	f, err := os.Open(src); if err != nil { return "", err }
	defer f.Close()

	r := csv.NewReader(f); r.FieldsPerRecord = -1
	firstRec, err := r.Read()
	if err != nil { return "", fmt.Errorf("empty file") }
	/* delimiter sniff */
	if strings.Count(strings.Join(firstRec, ""), ";") > strings.Count(strings.Join(firstRec, ""), ",") {
		r.Comma = ';'
	}

	var header []string
	cdr := ""
	if looksLikeHeader(firstRec) { header = firstRec } else { cdr = extractCDR(strings.Join(firstRec, " ")) }

	for header == nil {
		rec, er := r.Read(); if er == io.EOF { return "", fmt.Errorf("no header row") }
		if er != nil { continue }
		if cdr == "" { cdr = extractCDR(strings.Join(rec, " ")) }
		if looksLikeHeader(rec) { header = rec }
	}
	firstData, _ := r.Read()
	if cdr == "" {
		if i := colIdxAny(header, "msisdn", "msisdn number"); i != -1 && i < len(firstData) {
			cdr = digits(firstData[i])
		}
	}
	if cdr == "" { cdr = digits(filepath.Base(src)) }
	if cdr == "" { return "", fmt.Errorf("cannot find CDR") }

	/* map input→output columns */
	dstIx := func(col string) int {
		for i, h := range targetHeader { if h == col { return i } }; return -1
	}
	src2dst := map[int]int{}
	for i, h := range header {
		n := norm(h)
		/* via Headers.csv */
		if canon, ok := alias2canon[n]; ok {
			if d := dstIx(canon); d != -1 { src2dst[i] = d }
			continue
		}
		/* any header containing lrn */
		if strings.Contains(n, "lrn") { src2dst[i] = dstIx("LRN"); continue }
		/* call-type alias */
		if _, ok := callAlias[n]; ok { src2dst[i] = dstIx("Call Type"); continue }
		/* raw text match */
		for _, th := range targetHeader {
			if norm(th) == n { src2dst[i] = dstIx(th); break }
		}
	}

	/* extra synonyms */
	addMap := func(in, out string) {
		if i := colIdx(header, in); i != -1 { src2dst[i] = dstIx(out) }
	}
	addMap("call date", "Date")
	addMap("call initiation time", "Time")
	addMap("time", "Time")
	addMap("start time", "Time")
	addMap("call time", "Time")
	addMap("call duration", "Duration")
	addMap("b party number", "B Party")
	addMap("first cell global id", "First Cell ID")
	addMap("first bts location", "First Cell ID Address")
	addMap("last cell global id", "Last Cell ID")
	addMap("last bts location", "Last Cell ID Address")
	addMap("roaming network", "Roaming")
	addMap("roaming network/circle", "Roaming")
	addMap("roaming circle", "Roaming")
	addMap("service type", "Type")
	addMap("call forwarding", "CallForward")

	/* remember a call-type column if mapping failed */
	ctFallback := colIdxAny(header, "call type", "call_type", "type of call")

	/* ---- build rows & aggregators ---- */
	report := [][]string{targetHeader}

	/* summary aggregators (for 4 extra sheets) */
	type pAgg struct {
		Provider       string
		Calls, OutC, InC, OutS, InS int
		Dur            float64
		Dates, Cells   map[string]struct{}
		FirstDT, LastDT string
	}
	parties := map[string]*pAgg{}

	type cAgg struct {
		Addr, Lat, Lon, Az, Roam string
		Calls                    int
		FirstDT, LastDT          string
	}
	cells := map[string]*cAgg{}

	parseDT := func(d, t string) string {
		return strings.TrimSpace(d) + " " + strings.TrimSpace(t)
	}
	updateSpan := func(dt string, first *string, last *string) {
		if *first == "" || dt < *first { *first = dt }
		if *last == "" || dt > *last { *last = dt }
	}

	appendRow := func(rec []string) {
		if len(rec) == 0 { return }
		row := make([]string, len(targetHeader))

		for s, d := range src2dst {
			if s < len(rec) { row[d] = strings.Trim(rec[s], `"' `) }
		}
		/* fallbacks */
		if row[dstIx("Call Type")] == "" && ctFallback != -1 && ctFallback < len(rec) {
			row[dstIx("Call Type")] = strings.Trim(rec[ctFallback], `"' `)
		}

		/* post-processing */
		row[dstIx("CdrNo")]  = cdr
		row[dstIx("A Party")] = cdr
		row[dstIx("Operator(A-party)")] = "VI"
		/* if Roaming filled, copy to Circle(A-party) */
		row[dstIx("Circle(A-party)")] = row[dstIx("Roaming")]

		/* ensure TimeHH */
		if t := row[dstIx("Time")]; len(t) >= 2 { row[dstIx("TimeHH")] = t[:2] }
		row[dstIx("Crime")] = crime

		/* normalise B-Party number to last 10 digits */
		if bp := row[dstIx("B Party")]; bp != "" {
			row[dstIx("B Party")] = last10(bp)
		}
		/* tower look-up */
		fid := strings.ReplaceAll(row[dstIx("First Cell ID")], "-", "")
		lid := strings.ReplaceAll(row[dstIx("Last Cell ID")], "-", "")
		row[dstIx("First Cell ID")] = fid
		row[dstIx("Last Cell ID")]  = lid
		if addr, lat, lon, az, ok := lookupCell(fid); ok {
			if row[dstIx("First Cell ID Address")] == "" { row[dstIx("First Cell ID Address")] = addr }
			row[dstIx("Lat")] = lat; row[dstIx("Long")] = lon; row[dstIx("Azimuth")] = az
		}
		if addr, _, _, _, ok := lookupCell(lid); ok {
			if row[dstIx("Last Cell ID Address")] == "" { row[dstIx("Last Cell ID Address")] = addr }
		}

		/* LRN → Provider / Circle */
		if l := digits(row[dstIx("LRN")]); l != "" {
			if info, ok := lrnDB[l]; ok {
				row[dstIx("B Party Provider")] = info.Provider
				row[dstIx("B Party Circle")]   = info.Circle
			}
		}

		report = append(report, row)

		/* ---- summary aggregation ---- */
		bp := row[dstIx("B Party")]; if bp == "" { bp = "(blank)" }
		pa := parties[bp]
		if pa == nil {
			pa = &pAgg{Dates: map[string]struct{}{}, Cells: map[string]struct{}{}}
			parties[bp] = pa
		}
		if prov := row[dstIx("B Party Provider")]; prov != "" { pa.Provider = prov }
		pa.Calls++
		switch strings.ToUpper(row[dstIx("Call Type")]) {
		case "OUT", "CALL_OUT", "A_OUT": pa.OutC++
		case "IN", "CALL_IN", "A_IN": pa.InC++
		default:
			if strings.Contains(strings.ToUpper(row[dstIx("Call Type")]), "SMS") {
				if strings.Contains(strings.ToUpper(row[dstIx("Call Type")]), "OUT") { pa.OutS++ } else { pa.InS++ }
			}
		}
		if d, e := strconv.ParseFloat(row[dstIx("Duration")], 64); e == nil { pa.Dur += d }
		pa.Dates[row[dstIx("Date")]] = struct{}{}
		if fid != "" { pa.Cells[fid] = struct{}{} }

		dt := parseDT(row[dstIx("Date")], row[dstIx("Time")])
		updateSpan(dt, &pa.FirstDT, &pa.LastDT)

		/* cell agg (first ID only) */
		if fid != "" {
			ca := cells[fid]
			if ca == nil {
				ca = &cAgg{}
				cells[fid] = ca
			}
			ca.Calls++
			if ca.Addr == "" {
				ca.Addr = row[dstIx("First Cell ID Address")]
				ca.Lat = row[dstIx("Lat")]; ca.Lon = row[dstIx("Long")]; ca.Az = row[dstIx("Azimuth")]
				ca.Roam = row[dstIx("Roaming")]
			}
			updateSpan(dt, &ca.FirstDT, &ca.LastDT)
		}
	}

	appendRow(firstData)
	for {
		rec, er := r.Read(); if er == io.EOF { break }
		if er != nil || len(rec) == 0 { continue }
		appendRow(rec)
	}

	/* ------------ build extra worksheets ------------ */
	/* summary */
	summary := [][]string{{
		"CdrNo", "B Party", "Provider",
		"Total Calls", "Out Calls", "In Calls", "Out Sms", "In Sms",
		"Total Duration", "Total Days", "Total CellIds",
		"First Call", "Last Call",
	}}
	for bp, a := range parties {
		summary = append(summary, []string{
			cdr, bp, a.Provider,
			strconv.Itoa(a.Calls), strconv.Itoa(a.OutC), strconv.Itoa(a.InC),
			strconv.Itoa(a.OutS), strconv.Itoa(a.InS),
			fmt.Sprintf("%.0f", a.Dur),
			strconv.Itoa(len(a.Dates)), strconv.Itoa(len(a.Cells)),
			a.FirstDT, a.LastDT,
		})
	}

	/* max calls & max duration */
	type kv struct{ Key string; Val *pAgg }
	var plist []kv
	for k, v := range parties { plist = append(plist, kv{k, v}) }

	sort.Slice(plist, func(i, j int) bool { return plist[i].Val.Calls > plist[j].Val.Calls })
	maxCalls := [][]string{{"CdrNo", "B Party", "Total Calls", "Provider"}}
	for _, p := range plist {
		maxCalls = append(maxCalls, []string{cdr, p.Key, strconv.Itoa(p.Val.Calls), p.Val.Provider})
	}

	sort.Slice(plist, func(i, j int) bool { return plist[i].Val.Dur > plist[j].Val.Dur })
	maxDur := [][]string{{"CdrNo", "B Party", "Total Duration", "Provider"}}
	for _, p := range plist {
		maxDur = append(maxDur, []string{cdr, p.Key, fmt.Sprintf("%.0f", p.Val.Dur), p.Val.Provider})
	}

	/* max stay (cell) */
	type cellKV struct{ ID string; *cAgg }
	var clist []cellKV
	for id, v := range cells { clist = append(clist, cellKV{id, v}) }
	sort.Slice(clist, func(i, j int) bool { return clist[i].Calls > clist[j].Calls })
	maxStay := [][]string{{
		"CdrNo", "Cell ID", "Total Calls",
		"Tower Address", "Latitude", "Longitude", "Azimuth", "Roaming",
		"First Call", "Last Call",
	}}
	for _, c := range clist {
		maxStay = append(maxStay, []string{
			cdr, c.ID, strconv.Itoa(c.Calls),
			c.Addr, c.Lat, c.Lon, c.Az, c.Roam,
			c.FirstDT, c.LastDT,
		})
	}

	/* ---------------- write workbook ---------------- */
	x := excelize.NewFile()
	writeSheet := func(name string, rows [][]string, active bool) {
		idx, _ := x.NewSheet(name)
		for r, row := range rows {
			for c, v := range row {
				cell, _ := excelize.CoordinatesToCellName(c+1, r+1)
				_ = x.SetCellStr(name, cell, v)
			}
		}
		if active { x.SetActiveSheet(idx) }
	}
	writeSheet("report",      report,   true)
	writeSheet("summary",     summary,  false)
	writeSheet("max_calls",   maxCalls, false)
	writeSheet("max_duration",maxDur,   false)
	writeSheet("max_stay",    maxStay,  false)
	x.DeleteSheet("Sheet1")

	out := filepath.Join("filtered", cdr+"_vi_reports.xlsx")
	if err := x.SaveAs(out); err != nil { return "", err }
	return out, nil
}

/* header helpers */
func colIdxAny(h []string, keys ...string) int { for _, k := range keys { if i := colIdx(h, k); i != -1 { return i } }; return -1 }
func colIdx(h []string, key string) int {
	key = norm(key)
	for i, x := range h { if norm(x) == key { return i } }
	return -1
}
