// vi_filter.go
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
	"time"

	"github.com/xuri/excelize/v2"
)

/* ── canonical 26-column header ─────────────────────────── */
var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
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
func last10(s string) string {
	d := digits(s)
	if len(d) > 10 { return d[len(d)-10:] }
	if len(d) == 10 { return d }
	return ""
}

/* banner CDR extractor */
var msisdnRE = regexp.MustCompile(`(?i)msisdn[^0-9]*([0-9]{8,15})`)
func extractCDR(line string) string {
	if m := msisdnRE.FindStringSubmatch(line); len(m) > 1 { return m[1] }
	return ""
}

/* ── lookup tables & cell DB ── */
var (
	alias2canon = map[string]string{}
	callAlias   = map[string]struct{}{}
	lrnDB       = map[string]struct{ Provider, Circle, Operator string }{}
	cellDB      *sql.DB
)

/* ---------- init: load metadata ---------- */
func loadCSV(p string) [][]string {
	f, e := os.Open(p); if e != nil { return nil }
	defer f.Close()
	rows, _ := csv.NewReader(f).ReadAll()
	return rows
}
func init() {
	for _, r := range loadCSV(filepath.Join("vi", "data", "Headers.csv")) {
		if len(r) >= 2 {
			alias2canon[norm(r[0])] = r[1]
			alias2canon[norm(r[1])] = r[0]
		}
	}
	for _, r := range loadCSV(filepath.Join("vi", "data", "Call_types.csv")) {
		if len(r) > 0 { callAlias[norm(r[0])] = struct{}{} }
	}
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
	var err error
	cellDB, err = sql.Open("sqlite3",
		fmt.Sprintf("file:%s?mode=ro", filepath.Join("vi", "data", "testnewcellids.db")))
	if err != nil { panic(err) }
}
func lookupCell(id string) (addr, lat, lon, az string, ok bool) {
	const q = `SELECT address,latitude,longitude,azimuth FROM cellids
	           WHERE cellid=? OR REPLACE(cellid,'-','')=? LIMIT 1`
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
	_ = os.MkdirAll("uploads", 0o755); _ = os.MkdirAll("filtered", 0o755)
	src := filepath.Join("uploads", hdr.Filename)
	if err := saveFile(fh, src); err != nil { http.Error(w, err.Error(), 500); return }

	out, err := processVI(src, crime)
	if err != nil { http.Error(w, err.Error(), 500); return }
	fmt.Fprintf(w, "/download/%s\n", filepath.Base(out))
}
func saveFile(r io.Reader, dst string) error {
	f, err := os.Create(dst); if err != nil { return err }
	defer f.Close(); _, err = io.Copy(f, r); return err
}

/* ── core processor ─────────────────────────────────────── */
func processVI(src, crime string) (string, error) {
	f, err := os.Open(src); if err != nil { return "", err }
	defer f.Close()
	r := csv.NewReader(f)

	/* banner + header */
	var header []string; var cdr string
	for {
		rec, er := r.Read()
		if er == io.EOF { return "", fmt.Errorf("no header") }
		if er != nil { continue }
		if cdr == "" { cdr = extractCDR(strings.Join(rec, " ")) }
		if colIdx(rec, "call date") != -1 { header = rec; break }
	}
	first, _ := r.Read()
	if cdr == "" {
		if i := colIdxAny(header, "msisdn", "msisdn number"); i != -1 && i < len(first) {
			cdr = digits(first[i])
		}
	}
	if cdr == "" { cdr = digits(filepath.Base(src)) }
	if cdr == "" { return "", fmt.Errorf("cannot determine CDR") }

	/* ---------- column mapping ---------- */
	dstIdx := func(name string) int {
		for i, h := range targetHeader { if h == name { return i } }
		return -1
	}
	src2dst := map[int]int{}

	for i, h := range header {
		n := norm(h)

		if alias2canon[n] == "B Party" {
			if d := dstIdx("B Party"); d != -1 { src2dst[i] = d }

		} else if strings.Contains(n, "b party") && strings.Contains(n, "no") {
			if d := dstIdx("B Party"); d != -1 { src2dst[i] = d }

		} else if canon, ok := alias2canon[n]; ok {
			if d := dstIdx(canon); d != -1 { src2dst[i] = d }

		} else if _, ok := callAlias[n]; ok {
			if d := dstIdx("Call Type"); d != -1 { src2dst[i] = d }
		}
	}
	hard := func(key, canon string) {
		if i := colIdx(header, key); i != -1 {
			if d := dstIdx(canon); d != -1 { src2dst[i] = d }
		}
	}
	hard("call date", "Date"); hard("call initiation time", "Time")
	hard("call duration", "Duration"); hard("b party number", "B Party")
	hard("first cell global id", "First Cell ID"); hard("first bts location", "First Cell ID Address")
	hard("last cell global id", "Last Cell ID"); hard("last bts location", "Last Cell ID Address")
	hard("imei", "IMEI"); hard("imsi", "IMSI")
	hard("roaming network", "Roaming"); hard("lrn b party number", "LRN")
	hard("service type", "Type")

	/* ---------- buffers ---------- */
	report := [][]string{targetHeader}

	type pAgg struct {
		Calls, OutC, InC, OutS, InS int
		Dur                         float64
		Dates, Cells                map[string]struct{}
		Fd, Ft, Ld, Lt              string
	}
	parties := map[string]*pAgg{}

	type cAgg struct {
		Addr, Lat, Lon, Az, Roam    string
		Calls                       int
		Fd, Ft, Ld, Lt              string
	}
	cells := map[string]*cAgg{}
	updateDT := func(d, t string, fd, ft, ld, lt *string) {
		if *fd == "" || d < *fd || (d == *fd && t < *ft) { *fd, *ft = d, t }
		if *ld == "" || d > *ld || (d == *ld && t > *lt) { *ld, *lt = d, t }
	}

	/* ---------- per-row processing ---------- */
	process := func(rec []string) {
		if len(rec) == 0 { return }
		row := make([]string, len(targetHeader))
		for s, d := range src2dst {
			if d < 0 || s >= len(rec) { continue }
			val := strings.Trim(rec[s], `"' `)
			if d == dstIdx("B Party") { val = last10(val) }
			row[d] = val
		}
		row[dstIdx("CdrNo")] = cdr
		row[dstIdx("Crime")] = crime
		if t := row[dstIdx("Time")]; len(t) >= 2 { row[dstIdx("TimeHH")] = t[:2] }

		fid := strings.ReplaceAll(row[dstIdx("First Cell ID")], "-", "")
		lid := strings.ReplaceAll(row[dstIdx("Last Cell ID")], "-", "")
		row[dstIdx("First Cell ID")] = fid
		row[dstIdx("Last Cell ID")] = lid
		if addr, lat, lon, az, ok := lookupCell(fid); ok {
			row[dstIdx("First Cell ID Address")] = addr
			row[dstIdx("Lat")] = lat; row[dstIdx("Long")] = lon; row[dstIdx("Azimuth")] = az
		}
		if addr, _, _, _, ok := lookupCell(lid); ok {
			row[dstIdx("Last Cell ID Address")] = addr
		}

		if l := digits(row[dstIdx("LRN")]); l != "" {
			if info, ok := lrnDB[l]; ok {
				row[dstIdx("B Party Provider")] = info.Provider
				row[dstIdx("B Party Circle")] = info.Circle
			}
		}

		report = append(report, row)

		/* per-party aggregate */
		b := row[dstIdx("B Party")]; if b == "" { b = "(blank)" }
		pa := parties[b]; if pa == nil {
			pa = &pAgg{Dates: map[string]struct{}{}, Cells: map[string]struct{}{}}
			parties[b] = pa
		}
		pa.Calls++
		switch strings.ToUpper(row[dstIdx("Call Type")]) {
		case "OUT": pa.OutC++
		case "IN": pa.InC++
		case "OUT SMS": pa.OutS++
		case "IN SMS": pa.InS++
		}
		if d, e := strconv.ParseFloat(row[dstIdx("Duration")], 64); e == nil { pa.Dur += d }
		pa.Dates[row[dstIdx("Date")]] = struct{}{}
		if fid != "" { pa.Cells[fid] = struct{}{} }
		updateDT(row[dstIdx("Date")], row[dstIdx("Time")], &pa.Fd, &pa.Ft, &pa.Ld, &pa.Lt)

		/* per-cell aggregate */
		if fid != "" {
			ca := cells[fid]; if ca == nil { ca = &cAgg{}; cells[fid] = ca }
			ca.Calls++
			if ca.Addr == "" {
				ca.Addr = row[dstIdx("First Cell ID Address")]
				ca.Lat = row[dstIdx("Lat")]; ca.Lon = row[dstIdx("Long")]; ca.Az = row[dstIdx("Azimuth")]
				ca.Roam = row[dstIdx("Roaming")]
			}
			updateDT(row[dstIdx("Date")], row[dstIdx("Time")], &ca.Fd, &ca.Ft, &ca.Ld, &ca.Lt)
		}
	}

	process(first)
	for {
		rec, er := r.Read()
		if er == io.EOF { break }
		if er != nil || len(rec) == 0 { continue }
		process(rec)
	}

	/* ---------- summary / max sheets ---------- */
	summary := [][]string{{
		"CdrNo", "B Party", "Provider", "Type",
		"Total Calls", "Out Calls", "In Calls", "Out Sms", "In Sms",
		"Total Duration", "Total Days", "Total CellIds",
		"First Call Date", "First Call Time", "Last Call Date", "Last Call Time",
	}}
	for p, a := range parties {
		summary = append(summary, []string{
			cdr, p, "", "",
			strconv.Itoa(a.Calls),
			strconv.Itoa(a.OutC), strconv.Itoa(a.InC),
			strconv.Itoa(a.OutS), strconv.Itoa(a.InS),
			fmt.Sprintf("%.0f", a.Dur),
			strconv.Itoa(len(a.Dates)), strconv.Itoa(len(a.Cells)),
			a.Fd, a.Ft, a.Ld, a.Lt,
		})
	}
	type kv struct{ Party string; *pAgg }
	var list []kv
	for p, v := range parties { list = append(list, kv{p, v}) }
	sort.Slice(list, func(i, j int) bool { return list[i].Calls > list[j].Calls })
	maxC := [][]string{{"CdrNo", "B Party", "Total Calls", "Provider"}}
	for _, v := range list { maxC = append(maxC, []string{cdr, v.Party, strconv.Itoa(v.Calls), ""}) }
	sort.Slice(list, func(i, j int) bool { return list[i].Dur > list[j].Dur })
	maxD := [][]string{{"CdrNo", "B Party", "Total Duration", "Provider"}}
	for _, v := range list { maxD = append(maxD, []string{cdr, v.Party, fmt.Sprintf("%.0f", v.Dur), ""}) }

	/* max-stay */
	type cv struct{ ID string; *cAgg }
	var clist []cv
	for id, c := range cells { clist = append(clist, cv{id, c}) }
	sort.Slice(clist, func(i, j int) bool { return clist[i].Calls > clist[j].Calls })
	maxS := [][]string{{
		"CdrNo", "Cell ID", "Total Calls", "Days",
		"Tower Address", "Latitude", "Longitude", "Azimuth", "Roaming",
		"First Call Date", "First Call Time", "Last Call Date", "Last Call Time",
	}}
	for _, v := range clist {
		days := "-"
		if v.Fd != "" && v.Ld != "" {
			t1, _ := time.Parse("2006-01-02", v.Fd)
			t2, _ := time.Parse("2006-01-02", v.Ld)
			days = strconv.Itoa(int(t2.Sub(t1).Hours()/24) + 1)
		}
		maxS = append(maxS, []string{
			cdr, v.ID, strconv.Itoa(v.Calls), days,
			v.Addr, v.Lat, v.Lon, v.Az, v.Roam,
			v.Fd, v.Ft, v.Ld, v.Lt,
		})
	}

	/* ---------- workbook ---------- */
	x := excelize.NewFile()
	put := func(name string, rows [][]string, active bool) {
		idx, _ := x.NewSheet(name)
		for r, row := range rows {
			for c, v := range row {
				cell, _ := excelize.CoordinatesToCellName(c+1, r+1)
				x.SetCellStr(name, cell, v)
			}
		}
		if active { x.SetActiveSheet(idx) }
	}
	put("report", report, true)
	put("summary", summary, false)
	put("max_calls", maxC, false)
	put("max_duration", maxD, false)
	put("max_stay", maxS, false)
	x.DeleteSheet("Sheet1")
	out := filepath.Join("filtered", cdr+"_vi_all_reports.xlsx")
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
