package airtel

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
)

/* ───────── canonical 26‑column layout ───────── */
var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Main City(First CellID)", "Sub City (First CellID)", "Lat-Long-Azimuth (First CellID)",
	"Crime", "Circle", "Operator", "LRN",
	"CallForward", "B Party Provider", "B Party Circle", "B Party Operator",
	"Type", "IMEI Manufacturer",
}

/* ───────── header synonyms (input→canonical) ───────── */
var synonyms = map[string]string{
	"b party no": "B Party", "called party telephone number": "B Party",
	"call date": "Date", "date": "Date",
	"call time": "Time", "time": "Time",
	"dur(s)": "Duration", "call duration": "Duration",
	"imei": "IMEI", "imsi": "IMSI",
	"roam nw": "Roaming", "roaming circle name": "Circle",
	"circle": "Circle", "operator": "Operator",
	"lrn": "LRN", "lrn no": "LRN", "lrn number": "LRN", "lrn called no": "LRN",
	"call fow no": "CallForward", "call forwarding": "CallForward",
	"b party provider": "B Party Provider", "lrn tsp-lsa": "B Party Provider",
	"b party circle": "B Party Circle", "b party operator": "B Party Operator",
	"service type": "Type", "crime": "Crime",
}

/* ───────── misc helpers ───────── */
var (
	spaceRE  = regexp.MustCompile(`\s+`)
	nonDigit = regexp.MustCompile(`\D`)
)
func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }
func digits(s string) string { return nonDigit.ReplaceAllString(s, "") }
func cleanCGI(s string) string { return digits(s) }

/* ───────── banner‑line CDR extractors ───────── */
var (
	airtelCdrRE = regexp.MustCompile(`Mobile No '(\d+)'`)
)
func extractCdrNumber(content string) string {
	if m := airtelCdrRE.FindStringSubmatch(content); len(m) > 1 { return m[1] }
	return ""
}

/* ───────── embedded lookup data ───────── */
//go:embed data/*
var dataFS embed.FS

type CellInfo struct{ Address, SubCity, MainCity, LatLongAzimuth string }
type LRNInfo  struct{ Provider, Circle, Operator string }

var (
	cellDB = map[string]CellInfo{} // 15‑digit CGI → info
	lrnDB  = map[string]LRNInfo{}  // digits(LRN) → info
)

func init() {
	// cells
	if f, err := dataFS.Open("data/airtel_cells.csv"); err == nil { loadCells(f) } else {
		log.Fatalf("data/airtel_cells.csv missing: %v", err)
	}
	// LRN (optional)
	if f, err := dataFS.Open("data/LRN.csv"); err == nil { loadLRN(f) } else {
		log.Printf("data/LRN.csv not loaded: %v", err)
	}
}

/* ---------- load helpers ---------- */
func loadCells(f io.Reader) {
	r := csv.NewReader(f)
	hdr, _ := r.Read(); h := idx(hdr)
	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }
		id := strings.TrimSpace(rec[h["cell id"]]); if id == "" { continue }
		cellDB[id] = CellInfo{
			Address: rec[h["address"]],
			SubCity: rec[h["subcity"]],
			MainCity:rec[h["maincity"]],
			LatLongAzimuth: rec[h["latitude"]]+","+rec[h["longitude"]]+","+rec[h["azimuth"]],
		}
	}
}
func loadLRN(f io.Reader) {
	r := csv.NewReader(f)
	hdr, _ := r.Read(); h := idx(hdr)
	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec) == 0 { continue }
		key := digits(rec[h["lrn no"]]); if key == "" { continue }
		op := ""
		if _, ok := h["operator"]; ok { op = rec[h["operator"]] }
		lrnDB[key] = LRNInfo{
			Provider: rec[h["tsp"]],
			Circle:   rec[h["circle"]],
			Operator: op,
		}
	}
}
func idx(header []string) map[string]int { m := map[string]int{}; for i,h := range header { m[norm(h)]=i }; return m }

/* ───────────────────── HTTP handler ───────────────────── */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "POST only", 405); return }

	if strings.ToLower(r.FormValue("tsp_type")) != "airtel" {
		http.Error(w, "Only Airtel supported here", 400); return
	}
	crime := r.FormValue("crime_number")

	file, hdr, err := r.FormFile("file")
	if err != nil { http.Error(w, err.Error(), 400); return }
	defer file.Close()

	_ = os.MkdirAll("uploads", 0o755)
	_ = os.MkdirAll("filtered", 0o755)

	src := filepath.Join("uploads", hdr.Filename)
	if err := saveUploaded(file, src); err != nil { http.Error(w, err.Error(), 500); return }

	filtered, summary, err := normalizeAirtel(src, "filtered", crime)
	if err != nil { http.Error(w, err.Error(), 500); return }

	fmt.Fprintf(w, "/download/%s\n/download/%s\n", filepath.Base(filtered), filepath.Base(summary))
}
func saveUploaded(r io.Reader, dst string) error {
	f, err := os.Create(dst); if err != nil { return err }
	defer f.Close()
	_, err = io.Copy(f, r); return err
}

/* ───────── enrichment helpers ───────── */
func enrichCell(row []string, col map[string]int, id string, first bool) {
	if info, ok := cellDB[id]; ok {
		if first {
			row[col["First Cell ID Address"]]           = info.Address
			row[col["Sub City (First CellID)"]]         = info.SubCity
			row[col["Main City(First CellID)"]]         = info.MainCity
			row[col["Lat-Long-Azimuth (First CellID)"]] = info.LatLongAzimuth
		} else {
			row[col["Last Cell ID Address"]] = info.Address
		}
	}
}
func enrichLRN(row []string, col map[string]int) {
	lrn := digits(row[col["LRN"]]); if lrn == "" { return }
	if info, ok := lrnDB[lrn]; ok {
		if row[col["B Party Provider"]] == "" { row[col["B Party Provider"]] = info.Provider }
		row[col["B Party Circle"]]   = info.Circle
		if info.Operator != "" { row[col["B Party Operator"]] = info.Operator } else { row[col["B Party Operator"]] = info.Provider }
	}
}

/* ─────────── Airtel normaliser (filtered + summary) ─────────── */
func normalizeAirtel(src, dstDir, crime string) (filteredPath, summaryPath string, err error) {

	in, err := os.Open(src); if err != nil { return "", "", err }
	defer in.Close()
	r := csv.NewReader(in)

	/* locate header & CDR */
	var (
		header                []string
		cdrNum                string
		firstCGI, lastCGI     int
	)
	for {
		rec, er := r.Read()
		if er == io.EOF { return "", "", errors.New("no header") }
		if er != nil { continue }
		if cdrNum == "" { cdrNum = extractCdrNumber(rec[0]) }
		if len(rec) > 0 && strings.Contains(rec[0], "Target No") {
			header = rec; break
		}
	}
	if cdrNum == "" { return "", "", errors.New("cannot extract CDR") }

	colMap := make(map[string]int)
	for i, h := range targetHeader { colMap[h] = i }

	copyMap := map[int]int{}
	for i, h := range header {
		hN := norm(h)
		if hN == "first cgi" { firstCGI = i }
		if hN == "last cgi"  { lastCGI  = i }
		if canon, ok := synonyms[hN]; ok { copyMap[i] = colMap[canon] }
	}
	if firstCGI == 0 || lastCGI == 0 { return "", "", errors.New("missing First/Last CGI") }
	copyMap[firstCGI] = colMap["First Cell ID"]; copyMap[lastCGI] = colMap["Last Cell ID"]

	/* writers */
	filteredPath = filepath.Join(dstDir, cdrNum+"_reports.csv")
	fout, _ := os.Create(filteredPath); defer fout.Close()
	fw := csv.NewWriter(fout); fw.Write(targetHeader)
	blank := make([]string, len(targetHeader))

	/* summary aggregator */
	type agg struct {
		BParty, Provider, SDR, Type string
		Total, Out, In, OutSMS, InSMS, Other, RoamCall, RoamSMS int
		Dur float64
		Days, Cells, Imeis, Imsis map[string]struct{}
		First, Last string
	}
	summary := map[string]*agg{}
	parseDT := func(d,t string) string { return strings.TrimSpace(d)+" "+strings.TrimSpace(t) }

	for {
		rec, er := r.Read()
		if er == io.EOF { break }
		if er != nil || len(rec)==0 { continue }

		row := append([]string(nil), blank...)
		row[colMap["CdrNo"]] = cdrNum

		for s,d := range copyMap {
			if s >= len(rec) { continue }
			val := strings.Trim(rec[s],"'\" ")
			if targetHeader[d] == "Call Type" {
				switch strings.ToUpper(val) {
				case "IN","A_IN": val="CALL_IN"
				case "OUT","A_OUT": val="CALL_OUT"
				}
			}
			row[d] = val
		}
		row[colMap["Crime"]] = crime

		if first := cleanCGI(rec[firstCGI]); first!="" { row[colMap["First Cell ID"]]=first }
		if last  := cleanCGI(rec[lastCGI]);  last!=""  { row[colMap["Last Cell ID"]]=last }

		enrichCell(row,colMap,row[colMap["First Cell ID"]],true)
		enrichCell(row,colMap,row[colMap["Last Cell ID"]], false)
		enrichLRN(row,colMap)

		fw.Write(row)

		/* --- update summary --- */
		bKey := row[colMap["B Party"]]; if bKey=="" { bKey="(blank)" }
		a,ok := summary[bKey]; if !ok {
			a=&agg{BParty:bKey,Provider:row[colMap["B Party Provider"]],
				SDR:row[colMap["B Party Operator"]],Type:row[colMap["Type"]],
				Days:map[string]struct{}{},Cells:map[string]struct{}{},
				Imeis:map[string]struct{}{},Imsis:map[string]struct{}{}}
			summary[bKey]=a
		}
		a.Total++
		switch row[colMap["Call Type"]] {
		case "CALL_OUT": a.Out++
		case "CALL_IN":  a.In++
		default:
			if strings.Contains(row[colMap["Call Type"]],"SMS") {
				if strings.HasSuffix(row[colMap["Call Type"]],"OUT") { a.OutSMS++ } else { a.InSMS++ }
			} else { a.Other++ }
		}
		if row[colMap["Roaming"]]!="" {
			if strings.Contains(row[colMap["Call Type"]],"SMS") { a.RoamSMS++ } else { a.RoamCall++ }
		}
		if d,er:=strconv.ParseFloat(row[colMap["Duration"]],64);er==nil{ a.Dur+=d }
		a.Days[row[colMap["Date"]]]=struct{}{}
		if id:=row[colMap["First Cell ID"]];id!=""{a.Cells[id]=struct{}{}}
		if id:=row[colMap["Last Cell ID"]];id!=""{a.Cells[id]=struct{}{}}
		if v:=row[colMap["IMEI"]];v!=""{a.Imeis[v]=struct{}{}}
		if v:=row[colMap["IMSI"]];v!=""{a.Imsis[v]=struct{}{}}

		dt:=parseDT(row[colMap["Date"]],row[colMap["Time"]])
		if a.First==""||dt<a.First{a.First=dt}; if a.Last==""||dt>a.Last{a.Last=dt}
	}
	fw.Flush()

	/* summary file */
	summaryPath = filepath.Join(dstDir, cdrNum+"_summary_reports.csv")
	sout,_ := os.Create(summaryPath); defer sout.Close()
	sw := csv.NewWriter(sout)
	sw.Write([]string{
		"CdrNo","B Party","B Party SDR","Provider","Type",
		"Total Calls","Out Calls","In Calls","Out Sms","In Sms","Other Calls",
		"Roam Calls","Roam Sms","Total Duration","Total Days","Total CellIds",
		"Total Imei","Total Imsi","First Call","Last Call",
	})
	for _,a := range summary{
		sw.Write([]string{
			cdrNum,a.BParty,a.SDR,a.Provider,a.Type,
			fmt.Sprint(a.Total),fmt.Sprint(a.Out),fmt.Sprint(a.In),
			fmt.Sprint(a.OutSMS),fmt.Sprint(a.InSMS),fmt.Sprint(a.Other),
			fmt.Sprint(a.RoamCall),fmt.Sprint(a.RoamSMS),
			fmt.Sprintf("%.0f",a.Dur),
			fmt.Sprint(len(a.Days)),fmt.Sprint(len(a.Cells)),
			fmt.Sprint(len(a.Imeis)),fmt.Sprint(len(a.Imsis)),
			a.First,a.Last,
		})
	}
	sw.Flush()

	return filteredPath, summaryPath, nil
}
