package vi

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
	"strings"
)

/* ───────── 26‑column canonical layout (filtered) ───────── */
var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming",
	"Main City(First CellID)", "Sub City (First CellID)", "Lat-Long-Azimuth (First CellID)",
	"Crime", "Circle", "Operator", "LRN",
	"CallForward", "B Party Provider", "B Party Circle", "B Party Operator",
	"Type", "IMEI Manufacturer",
}

/* ───────── small helpers ───────── */
var (
	spaceRE  = regexp.MustCompile(`\s+`)
	nonDigit = regexp.MustCompile(`\D`)
)
func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }
func digits(s string) string { return nonDigit.ReplaceAllString(s, "") }

/* first matching index */
func colIdx(header []string, key string) int { return colIdxAny(header, key) }
func colIdxAny(header []string, keys ...string) int {
	for _, k := range keys {
		for i, h := range header {
			if norm(h) == norm(k) { return i }
		}
	}
	return -1
}

/* ── “MSISDN … ” banner extractor ── */
var msisdnRE = regexp.MustCompile(`(?i)msisdn[^0-9]*([0-9]{8,15})`)
func extractCdrNumber(line string) string { if m:=msisdnRE.FindStringSubmatch(line);len(m)>1{return m[1]};return "" }

/* ───────── embedded reference CSVs ───────── */
//go:embed data/*
var dataFS embed.FS

type CellInfo struct{ Addr, Sub, Main, LatLonAz string }
type LRNInfo  struct{ Provider, Circle, Operator string }

var (
	cellDB = map[string]map[string]CellInfo{} // tsp→map[id]info
	lrnDB  = map[string]LRNInfo{}
)

func init() {
	_ = loadCells("vi", "data/vi_cells.csv")
	_ = loadLRN("data/LRN.csv") // best‑effort
}

/* ---------- loadCells ---------- */
func loadCells(tsp, path string) error {
	f, err := dataFS.Open(path); if err != nil { return err }
	defer f.Close()

	r := csv.NewReader(f)
	hdr, _ := r.Read()
	idx := func(keys ...string) int { return colIdxAny(hdr, keys...) }

	iID := idx("cgi", "cell global id", "cell id")
	iAddr := idx("address", "bts location")
	iSub  := idx("subcity", "sub city")
	iMain := idx("maincity", "city")
	iLat, iLon, iAz := idx("latitude"), idx("longitude", "lon"), idx("azimuth", "az")

	if iID == -1 { return fmt.Errorf("no CGI column in %s", path) }
	cellDB[tsp] = map[string]CellInfo{}

	for {
		rec, err := r.Read()
		if err == io.EOF { break }
		if err != nil || len(rec)==0 { continue }

		raw := strings.TrimSpace(rec[iID]); if raw=="" { continue }
		info := CellInfo{
			Addr: pick(rec,iAddr), Sub: pick(rec,iSub), Main: pick(rec,iMain),
			LatLonAz: buildLat(rec,iLat,iLon,iAz),
		}
		cellDB[tsp][raw] = info
		cellDB[tsp][digits(raw)] = info
	}
	return nil
}

/* ---------- loadLRN ---------- */
func loadLRN(path string) error {
	f, err := dataFS.Open(path); if err != nil { return err }
	defer f.Close()

	r := csv.NewReader(f)
	hdr, _ := r.Read()
	iLRN := colIdxAny(hdr,"lrn","lrn no")
	iTSP := colIdxAny(hdr,"tsp","provider")
	iCircle := colIdxAny(hdr,"circle")
	if iLRN == -1 || iTSP == -1 { return fmt.Errorf("LRN.csv missing columns") }

	for {
		rec,err:=r.Read()
		if err==io.EOF { break }
		if err!=nil||len(rec)==0 { continue }
		key := digits(rec[iLRN]); if key=="" { continue }
		lrnDB[key]=LRNInfo{Provider:rec[iTSP],Circle:pick(rec,iCircle),Operator:rec[iTSP]}
	}
	return nil
}

/* ---------- utilities ---------- */
func pick(rec []string, idx int) string {
	if idx==-1||idx>=len(rec) { return "" }
	return strings.TrimSpace(rec[idx])
}
func buildLat(rec []string,iLat,iLon,iAz int) string {
	if iLat==-1||iLon==-1 { return "" }
	lat,lon:=pick(rec,iLat),pick(rec,iLon)
	if lat==""||lon=="" { return "" }
	if az:=pick(rec,iAz);az!=""{ return lat+", "+lon+", "+az }
	return lat+", "+lon
}
func findCell(tsp,id string)(CellInfo,bool){
	db:=cellDB[tsp]
	if info,ok:=db[id];ok{return info,true}
	if info,ok:=db[digits(id)];ok{return info,true}
	return CellInfo{},false
}

/* ───────────────────── HTTP handler ───────────────────── */
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request){
	if r.Method!=http.MethodPost{http.Error(w,"POST only",405);return}
	if strings.ToLower(r.FormValue("tsp_type"))!="vi"{http.Error(w,"Only VI supported",400);return}
	crime:=r.FormValue("crime_number")

	file,hdr,err:=r.FormFile("file"); if err!=nil{http.Error(w,err.Error(),400);return}
	defer file.Close()

	_ = os.MkdirAll("uploads",0o755); _ = os.MkdirAll("filtered",0o755)
	src:=filepath.Join("uploads",hdr.Filename)
	if err:=saveUploaded(file,src);err!=nil{http.Error(w,err.Error(),500);return}

	filtered,summary,err:=normVI(src,crime)
	if err!=nil{http.Error(w,err.Error(),500);return}

	fmt.Fprintf(w,"/download/%s\n/download/%s\n",filepath.Base(filtered),filepath.Base(summary))
}
func saveUploaded(r io.Reader,dst string) error { f,err:=os.Create(dst); if err!=nil{return err}; defer f.Close();_,err=io.Copy(f,r);return err }

/* ─────────────── VI normaliser (filtered + summary) ─────────────── */
func normVI(src,crime string)(filteredPath,summaryPath string,err error){
	in,err:=os.Open(src); if err!=nil{return "","",err}; defer in.Close()
	r:=csv.NewReader(in)

	/* locate header & CDR */
	var header []string
	var cdr string
	for{
		rec,er:=r.Read()
		if er==io.EOF{ return "","",errors.New("no header") }
		if er!=nil{ continue }
		if cdr==""{ cdr = extractCdrNumber(strings.Join(rec," ")) }
		if colIdx(rec,"call date")!=-1{ header=rec; break }
	}

	firstData,er:=r.Read(); if er!=nil{ return "","",errors.New("header only") }
	if cdr==""{
		if idx:=colIdxAny(header,"msisdn"); idx!=-1 && idx<len(firstData){
			cdr = digits(firstData[idx])
		}
	}
	if cdr==""{ nameDigits:=digits(filepath.Base(src)); if len(nameDigits)>=10&&len(nameDigits)<=15{ cdr=nameDigits } }
	if cdr==""{ return "","",errors.New("cannot find CDR") }

	/* indices */
	idxDate := colIdx(header,"call date")
	idxTime := colIdx(header,"call initiation time")
	idxDur  := colIdxAny(header,"call duration","duration(sec)","duration")
	idxB    := colIdxAny(header,"b party number","b party no")
	idxType := colIdx(header,"call_type")
	idxFirstID := colIdxAny(header,"first cell global id")
	idxFirstAddr := colIdxAny(header,"first bts location")
	idxLastID  := colIdxAny(header,"last cell global id")
	idxLastAddr:= colIdxAny(header,"last bts location")
	idxIMEI := colIdx(header,"imei")
	idxIMSI := colIdx(header,"imsi")
	idxRoam := colIdxAny(header,"roaming network","roaming network/circle")
	idxLRN  := colIdxAny(header,"lrn- b party number","lrn b party number")
	idxSrv  := colIdx(header,"service type")

	/* writers */
	filteredPath = filepath.Join("filtered",cdr+"_reports.csv")
	fout,_:=os.Create(filteredPath); defer fout.Close()
	fw:=csv.NewWriter(fout); fw.Write(targetHeader)
	col:=map[string]int{};for i,h:=range targetHeader{col[h]=i}
	blank:=make([]string,len(targetHeader))

	/* summary */
	type agg struct{
		BParty,Provider,SDR,Type string
		Total,Out,In,OutSMS,InSMS,Other,RoamCall,RoamSMS int
		Dur float64
		Days,Cells,Imeis,Imsis map[string]struct{}
		First,Last string
	}
	summary:=map[string]*agg{}
	parseDT:=func(d,t string)string{ return strings.TrimSpace(d)+" "+strings.TrimSpace(t) }

	writeRow:=func(rec []string){
		if len(rec)==0{ return }
		row:=append([]string(nil),blank...)
		row[col["CdrNo"]]=cdr; row[col["Crime"]]=crime

		cp:=func(src int,dst string){ if src!=-1&&src<len(rec){ row[col[dst]]=strings.Trim(rec[src],"'\" ") }}
		cp(idxDate,"Date"); cp(idxTime,"Time"); cp(idxDur,"Duration")
		cp(idxB,"B Party"); cp(idxType,"Call Type")
		cp(idxFirstID,"First Cell ID"); cp(idxFirstAddr,"First Cell ID Address")
		cp(idxLastID,"Last Cell ID");  cp(idxLastAddr,"Last Cell ID Address")
		cp(idxIMEI,"IMEI"); cp(idxIMSI,"IMSI"); cp(idxRoam,"Roaming")
		cp(idxLRN,"LRN"); cp(idxSrv,"Type")

		/* cell enrich */
		if id:=pick(rec,idxFirstID); id!=""{ if info,ok:=findCell("vi",id);ok{
			if row[col["Main City(First CellID)"]]==""{ row[col["Main City(First CellID)"]]=info.Main }
			if row[col["Sub City (First CellID)"]]==""{ row[col["Sub City (First CellID)"]]=info.Sub }
			if row[col["Lat-Long-Azimuth (First CellID)"]]==""{ row[col["Lat-Long-Azimuth (First CellID)"]]=info.LatLonAz }
			if row[col["First Cell ID Address"]]==""{ row[col["First Cell ID Address"]]=info.Addr }
		}}
		/* LRN enrich */
		if l:=digits(row[col["LRN"]]);l!=""{ if info,ok:=lrnDB[l];ok{
			row[col["B Party Provider"]]=info.Provider
			row[col["B Party Circle"]]=info.Circle
			row[col["B Party Operator"]]=info.Operator
		}}

		fw.Write(row)

		/* summary update */
		bKey:=row[col["B Party"]]; if bKey==""{ bKey="(blank)" }
		a,ok:=summary[bKey]
		if !ok{
			a=&agg{BParty:bKey,Provider:row[col["B Party Provider"]],
				SDR:row[col["B Party Operator"]],Type:row[col["Type"]],
				Days:map[string]struct{}{},Cells:map[string]struct{}{},
				Imeis:map[string]struct{}{},Imsis:map[string]struct{}{}}
			summary[bKey]=a
		}
		a.Total++
		switch row[col["Call Type"]]{
		case "CALL_OUT":a.Out++
		case "CALL_IN": a.In++
		default:
			if strings.Contains(row[col["Call Type"]],"SMS"){
				if strings.HasSuffix(row[col["Call Type"]],"OUT"){ a.OutSMS++ } else { a.InSMS++ }
			}else{ a.Other++ }
		}
		if row[col["Roaming"]]!=""{
			if strings.Contains(row[col["Call Type"]],"SMS"){ a.RoamSMS++ } else { a.RoamCall++ }
		}
		if d,er:=strconv.ParseFloat(row[col["Duration"]],64);er==nil{ a.Dur+=d }
		a.Days[row[col["Date"]]]=struct{}{}
		if id:=row[col["First Cell ID"]];id!=""{a.Cells[id]=struct{}{}}
		if id:=row[col["Last Cell ID"]];id!=""{a.Cells[id]=struct{}{}}
		if v:=row[col["IMEI"]];v!=""{a.Imeis[v]=struct{}{}}
		if v:=row[col["IMSI"]];v!=""{a.Imsis[v]=struct{}{}}
		dt:=parseDT(row[col["Date"]],row[col["Time"]])
		if a.First==""||dt<a.First{a.First=dt}; if a.Last==""||dt>a.Last{a.Last=dt}
	}

	/* first row plus remaining */
	writeRow(firstData)
	for{
		rec,er:=r.Read()
		if er==io.EOF{ break }
		if er!=nil||len(rec)==0{ continue }
		writeRow(rec)
	}
	fw.Flush()

	/* summary writer */
	summaryPath = filepath.Join("filtered",cdr+"_summary_reports.csv")
	sout,_:=os.Create(summaryPath); defer sout.Close()
	sw:=csv.NewWriter(sout)
	sw.Write([]string{
		"CdrNo","B Party","B Party SDR","Provider","Type",
		"Total Calls","Out Calls","In Calls","Out Sms","In Sms",
		"Other Calls","Roam Calls","Roam Sms","Total Duration",
		"Total Days","Total CellIds","Total Imei","Total Imsi",
		"First Call","Last Call",
	})
	for _,a:=range summary{
		sw.Write([]string{
			cdr,a.BParty,a.SDR,a.Provider,a.Type,
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

	return filteredPath,summaryPath,nil
}
