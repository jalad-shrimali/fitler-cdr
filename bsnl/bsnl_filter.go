package bsnl

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

/* ───────── canonical 26-column layout ───────── */
var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Lat", "Long", "Azimuth",
	"Crime", "Circle(A-party)", "Operator(A-party)", "LRN",
	"CallForward", "B Party Provider", "B Party Circle",
	"Type", "IMEI Manufacturer", "TimeHH",
}

/* ───────── helpers ───────── */
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

/* banner extractor */
var searchValRE = regexp.MustCompile(`(?i)search\s*value[^0-9]*([0-9]{8,15})`)
func extractCDR(line string) string {
	if m := searchValRE.FindStringSubmatch(line); len(m) > 1 {
		return m[1]
	}
	return ""
}

/* lookup tables (Headers, Call_types, LRN) + cell DB */
var (
	alias2canon = map[string]string{}
	callAlias   = map[string]struct{}{}
	lrnDB       = map[string]struct{ Provider, Circle, Operator string }{}
	cellDB      *sql.DB
)

func init() { loadMeta(); openCellDB() }

func loadCSV(path string) [][]string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	rows, _ := csv.NewReader(f).ReadAll()
	return rows
}
func loadMeta() {
	for _, r := range loadCSV(filepath.Join("bsnl", "data", "Headers.csv")) {
		if len(r) >= 2 {
			alias2canon[norm(r[0])] = r[1]
			alias2canon[norm(r[1])] = r[0]
		}
	}
	for _, r := range loadCSV(filepath.Join("bsnl", "data", "Call_types.csv")) {
		if len(r) > 0 {
			callAlias[norm(r[0])] = struct{}{}
		}
	}
	rows := loadCSV(filepath.Join("bsnl", "data", "LRN.csv"))
	if len(rows) > 1 {
		h := rows[0]
		idx := func(keys ...string) int {
			for i, c := range h {
				for _, k := range keys {
					if norm(c) == norm(k) {
						return i
					}
				}
			}
			return -1
		}
		iLRN, iTSP, iCir := idx("lrn", "lrn no"), idx("tsp", "provider"), idx("circle")
		for _, r := range rows[1:] {
			key := digits(r[iLRN])
			if key == "" {
				continue
			}
			lrnDB[key] = struct{ Provider, Circle, Operator string }{
				strings.TrimSpace(r[iTSP]),
				strings.TrimSpace(r[iCir]),
				strings.TrimSpace(r[iTSP]),
			}
		}
	}
}

func openCellDB() {
	dbPath := filepath.Join("bsnl", "data", "testnewcellids.db")
	var err error
	cellDB, err = sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", dbPath))
	if err != nil {
		panic(err)
	}
}
func lookupCell(id string) (addr, lat, lon, az string, ok bool) {
	const q = `SELECT address,latitude,longitude,azimuth FROM cellids
	           WHERE cellid=? OR REPLACE(cellid,'-','')=? LIMIT 1`
	err := cellDB.QueryRow(q, id, id).Scan(&addr, &lat, &lon, &az)
	return addr, lat, lon, az, err == nil
}

/* ───────── HTTP handler ───────── */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	if norm(r.FormValue("tsp_type")) != "bsnl" {
		http.Error(w, "Only BSNL", 400)
		return
	}
	crime := r.FormValue("crime_number")

	fh, hdr, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer fh.Close()
	_ = os.MkdirAll("uploads", 0o755)
	_ = os.MkdirAll("filtered", 0o755)
	src := filepath.Join("uploads", hdr.Filename)
	if err := save(fh, src); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	out, err := processBSNL(src, crime)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	fmt.Fprintf(w, "/download/%s\n", filepath.Base(out))
}
func save(r io.Reader, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

/* ───────── core processor ───────── */
func processBSNL(src, crime string) (string, error) {
	f, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer f.Close()
	r := csv.NewReader(f)

	/* header + banner */
	var header []string
	var cdr string
	for {
		rec, er := r.Read()
		if er == io.EOF {
			return "", fmt.Errorf("no header")
		}
		if er != nil {
			continue
		}
		if cdr == "" {
			cdr = extractCDR(strings.Join(rec, " "))
		}
		if colIdx(rec, "call_date") != -1 {
			header = rec
			break
		}
	}
	first, _ := r.Read()
	if cdr == "" {
		if idx := colIdxAny(header, "search value"); idx != -1 && idx < len(first) {
			cdr = digits(first[idx])
		}
	}
	if cdr == "" {
		cdr = digits(filepath.Base(src))
	}
	if cdr == "" {
		return "", fmt.Errorf("cannot determine CDR")
	}

	/* dst index map */
	dst := map[string]int{}
	for i, h := range targetHeader {
		dst[h] = i
	}
	src2dst := map[int]int{}
	var bCols []int
	for i, h := range header {
		n := norm(h)
		if canon, ok := alias2canon[n]; ok && canon == "B Party" {
			src2dst[i] = dst["B Party"]
			bCols = append(bCols, i)
		} else if strings.Contains(n, "b party") && strings.Contains(n, "no") {
			src2dst[i] = dst["B Party"]
			bCols = append(bCols, i)
		} else if canon, ok := alias2canon[n]; ok {
			src2dst[i] = dst[canon]
		} else if _, ok := callAlias[n]; ok {
			src2dst[i] = dst["Call Type"]
		}
	}

	/* helper: map col if not already mapped */
	mapCol := func(key, canon string) {
		if idx := colIdx(header, key); idx != -1 && src2dst[idx] == 0 {
			src2dst[idx] = dst[canon]
		}
	}
	mapCol("call_date", "Date")
	mapCol("call_initiation_time", "Time")
	mapCol("call_duration", "Duration")
	mapCol("other_party_no", "B Party")
	mapCol("call_type", "Call Type")
	mapCol("first_cell_id", "First Cell ID")
	mapCol("last_cell_id", "Last Cell ID")
	mapCol("imei", "IMEI")
	mapCol("imsi", "IMSI")
	mapCol("roaming circle", "Roaming")
	mapCol("lrn_b_party_no", "LRN")
	mapCol("call_forward", "CallForward")
	mapCol("service_type", "Type")

	/* excel workbook */
	x := excelize.NewFile()
	addSheet := func(name string, rows [][]string, active bool) {
		idx, _ := x.NewSheet(name)
		for r, row := range rows {
			for c, v := range row {
				cell, _ := excelize.CoordinatesToCellName(c+1, r+1)
				x.SetCellStr(name, cell, v)
			}
		}
		if active {
			x.SetActiveSheet(idx)
		}
	}
	report := [][]string{targetHeader}

	/* aggregations */
	type partyAgg struct {
		Provider                     string
		Calls, OutC, InC, OutS, InS  int
		Dur                          float64
		Dates                        map[string]struct{}
		Cells                        map[string]struct{}
		Fd, Ft, Ld, Lt               string
	}
	parties := map[string]*partyAgg{}

	type cellAgg struct {
		Addr, Lat, Lon, Az, Roam     string
		Calls                        int
		Fd, Ft, Ld, Lt               string
	}
	cells := map[string]*cellAgg{}
	updateDT := func(d, t string, fd, ft, ld, lt *string) {
		if *fd == "" || d < *fd || (d == *fd && t < *ft) {
			*fd, *ft = d, t
		}
		if *ld == "" || d > *ld || (d == *ld && t > *lt) {
			*ld, *lt = d, t
		}
	}

	process := func(rec []string) {
		if len(rec) == 0 {
			return
		}
		row := make([]string, len(targetHeader))
		for s, d := range src2dst {
			if s < len(rec) {
				val := strings.Trim(rec[s], `"' `)
				if d == dst["B Party"] {
					if dig := last10(val); dig != "" && dig != cdr {
						val = dig
					}
				}
				row[d] = val
			}
		}
		row[dst["CdrNo"]] = cdr
		row[dst["Crime"]] = crime
		if t := row[dst["Time"]]; len(t) >= 2 {
			row[dst["TimeHH"]] = t[:2]
		}

		/* cell enrichment */
		fid := strings.ReplaceAll(row[dst["First Cell ID"]], "-", "")
		lid := strings.ReplaceAll(row[dst["Last Cell ID"]], "-", "")
		row[dst["First Cell ID"]] = fid
		row[dst["Last Cell ID"]] = lid
		if addr, lat, lon, az, ok := lookupCell(fid); ok {
			row[dst["First Cell ID Address"]] = addr
			row[dst["Lat"]] = lat
			row[dst["Long"]] = lon
			row[dst["Azimuth"]] = az
		}
		if addr, _, _, _, ok := lookupCell(lid); ok {
			row[dst["Last Cell ID Address"]] = addr
		}

		/* LRN enrichment */
		if l := digits(row[dst["LRN"]]); l != "" {
			if info, ok := lrnDB[l]; ok {
				row[dst["B Party Provider"]] = info.Provider
				row[dst["B Party Circle"]] = info.Circle
			}
		}

		report = append(report, row)

		/* party agg */
		b := row[dst["B Party"]]
		if b == "" {
			b = "(blank)"
		}
		pa := parties[b]
		if pa == nil {
			pa = &partyAgg{Dates: map[string]struct{}{}, Cells: map[string]struct{}{}}
			parties[b] = pa
		}
		pa.Calls++
		switch strings.ToUpper(row[dst["Call Type"]]) {
		case "OUT":
			pa.OutC++
		case "IN":
			pa.InC++
		case "OUT SMS":
			pa.OutS++
		case "IN SMS":
			pa.InS++
		}
		if d, e := strconv.ParseFloat(row[dst["Duration"]], 64); e == nil {
			pa.Dur += d
		}
		pa.Dates[row[dst["Date"]]] = struct{}{}
		pa.Cells[fid] = struct{}{}
		pa.Cells[lid] = struct{}{}
		updateDT(row[dst["Date"]], row[dst["Time"]], &pa.Fd, &pa.Ft, &pa.Ld, &pa.Lt)

		/* cell agg */
		if fid != "" {
			ca := cells[fid]
			if ca == nil {
				ca = &cellAgg{}
				cells[fid] = ca
			}
			ca.Calls++
			if ca.Addr == "" {
				ca.Addr = row[dst["First Cell ID Address"]]
				ca.Lat = row[dst["Lat"]]
				ca.Lon = row[dst["Long"]]
				ca.Az = row[dst["Azimuth"]]
				ca.Roam = row[dst["Roaming"]]
			}
			updateDT(row[dst["Date"]], row[dst["Time"]], &ca.Fd, &ca.Ft, &ca.Ld, &ca.Lt)
		}
	}

	process(first)
	for {
		rec, er := r.Read()
		if er == io.EOF {
			break
		}
		if er != nil || len(rec) == 0 {
			continue
		}
		process(rec)
	}

	/* summary sheet */
	summary := [][]string{{
		"CdrNo", "B Party", "Provider", "Type",
		"Total Calls", "Out Calls", "In Calls", "Out Sms", "In Sms",
		"Total Duration", "Total Days", "Total CellIds",
		"First Call Date", "First Call Time", "Last Call Date", "Last Call Time",
	}}
	for p, a := range parties {
		summary = append(summary, []string{
			cdr, p, a.Provider, "",
			strconv.Itoa(a.Calls),
			strconv.Itoa(a.OutC), strconv.Itoa(a.InC),
			strconv.Itoa(a.OutS), strconv.Itoa(a.InS),
			fmt.Sprintf("%.0f", a.Dur),
			strconv.Itoa(len(a.Dates)),
			strconv.Itoa(len(a.Cells)),
			a.Fd, a.Ft, a.Ld, a.Lt,
		})
	}

	/* max_calls & max_duration sheets */
	type kv struct{ Party string; *partyAgg }
	var list []kv
	for p, v := range parties {
		list = append(list, kv{p, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Calls > list[j].Calls })
	maxC := [][]string{{"CdrNo", "B Party", "Total Calls", "Provider"}}
	for _, v := range list {
		maxC = append(maxC, []string{cdr, v.Party, strconv.Itoa(v.Calls), v.Provider})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Dur > list[j].Dur })
	maxD := [][]string{{"CdrNo", "B Party", "Total Duration", "Provider"}}
	for _, v := range list {
		maxD = append(maxD, []string{cdr, v.Party, fmt.Sprintf("%.0f", v.Dur), v.Provider})
	}

	/* max_stay sheet */
	type cellkv struct{ ID string; *cellAgg }
	var clist []cellkv
	for id, c := range cells {
		clist = append(clist, cellkv{id, c})
	}
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

	/* write workbook */
	addSheet("report", report, true)
	addSheet("summary", summary, false)
	addSheet("max_calls", maxC, false)
	addSheet("max_duration", maxD, false)
	addSheet("max_stay", maxS, false)
	x.DeleteSheet("Sheet1")
	out := filepath.Join("filtered", cdr+"_bsnl_all_reports.xlsx")
	if err := x.SaveAs(out); err != nil {
		return "", err
	}
	return out, nil
}

/* header helpers */
func colIdxAny(h []string, keys ...string) int {
	for _, k := range keys {
		if i := colIdx(h, k); i != -1 {
			return i
		}
	}
	return -1
}
func colIdx(h []string, key string) int {
	key = norm(key)
	for i, x := range h {
		if norm(x) == key {
			return i
		}
	}
	return -1
}
