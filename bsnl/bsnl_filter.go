package bsnl

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

/* ───────── helpers ───────── */
var (
	spaceRE  = regexp.MustCompile(`\s+`)
	nonDigit = regexp.MustCompile(`\D`)
)
func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }
func digits(s string) string { return nonDigit.ReplaceAllString(s, "") }

/* column index helpers */
func colIdx(header []string, key string) int { return colIdxAny(header, key) }
func colIdxAny(header []string, keys ...string) int {
	for _, k := range keys {
		for i, h := range header {
			if norm(h) == norm(k) { return i }
		}
	}
	return -1
}

/* ── "Search Value :" banner → CDR ── */
var searchValRE = regexp.MustCompile(`(?i)search\s*value[^0-9]*([0-9]{8,15})`)
func extractCDR(line string) string { if m:=searchValRE.FindStringSubmatch(line); len(m)>1 { return m[1] }; return "" }

/* ───────── embedded reference data ───────── */
//go:embed data/*
var dataFS embed.FS
type CellInfo struct{ Addr, Sub, Main, LatLonAz string }
type LRNInfo  struct{ Provider, Circle, Operator string }

var (
	cellDB = map[string]CellInfo{} // id→info
	lrnDB  = map[string]LRNInfo{}  // digits(lrn)→info
)

func init() { loadCells("data/bsnl_cells.csv"); loadLRN("data/LRN.csv") }

/* ---------- loadCells ---------- */
func loadCells(path string){
	f,err:=dataFS.Open(path); if err!=nil{log.Printf("warning: %v",err);return}
	defer f.Close()
	r:=csv.NewReader(f); hdr,_:=r.Read()
	iID := colIdxAny(hdr,"cgi","cell id","cell_id")
	iAddr:=colIdxAny(hdr,"address"); iSub:=colIdxAny(hdr,"subcity")
	iMain:=colIdxAny(hdr,"maincity","city"); iLat:=colIdxAny(hdr,"latitude")
	iLon:=colIdxAny(hdr,"longitude","lon"); iAz:=colIdxAny(hdr,"azimuth","az")
	for{
		rec,er:=r.Read(); if er==io.EOF{break}; if er!=nil||len(rec)==0{continue}
		raw:=strings.TrimSpace(rec[iID]); if raw==""{continue}
		info:=CellInfo{
			Addr: pick(rec,iAddr), Sub: pick(rec,iSub), Main: pick(rec,iMain),
			LatLonAz: buildLat(rec,iLat,iLon,iAz),
		}
		cellDB[raw]=info; cellDB[digits(raw)]=info
	}
}

/* ---------- loadLRN ---------- */
func loadLRN(path string){
	f,err:=dataFS.Open(path); if err!=nil{log.Printf("warning: %v",err);return}
	defer f.Close()
	r:=csv.NewReader(f); hdr,_:=r.Read()
	iLRN:=colIdxAny(hdr,"lrn","lrn no"); iTSP:=colIdxAny(hdr,"tsp","provider")
	iCircle:=colIdxAny(hdr,"circle")
	for{
		rec,er:=r.Read(); if er==io.EOF{break}; if er!=nil||len(rec)==0{continue}
		key:=digits(rec[iLRN]); if key==""{continue}
		lrnDB[key]=LRNInfo{Provider:rec[iTSP],Circle:pick(rec,iCircle),Operator:rec[iTSP]}
	}
}

/* utilities */
func pick(rec []string, idx int) string { if idx==-1||idx>=len(rec){return""}; return strings.TrimSpace(rec[idx]) }
func buildLat(rec []string,iLat,iLon,iAz int)string{
	if iLat==-1||iLon==-1{ return"" }
	lat,lon:=pick(rec,iLat),pick(rec,iLon); if lat==""||lon==""{return""}
	if az:=pick(rec,iAz);az!=""{return lat+", "+lon+", "+az}; return lat+", "+lon
}
func cellLookup(id string)(CellInfo,bool){
	if info,ok:=cellDB[id];ok{return info,true}
	if info,ok:=cellDB[digits(id)];ok{return info,true}
	return CellInfo{},false
}

/* ───────────────────── HTTP handler ───────────────────── */
func UploadAndNormalizeCSV(w http.ResponseWriter,r *http.Request){
	if r.Method!=http.MethodPost{http.Error(w,"POST only",405);return}
	if strings.ToLower(r.FormValue("tsp_type"))!="bsnl"{http.Error(w,"Only BSNL supported",400);return}
	crime:=r.FormValue("crime_number")

	fh,hdr,err:=r.FormFile("file"); if err!=nil{http.Error(w,err.Error(),400);return}
	defer fh.Close()
	_ = os.MkdirAll("uploads",0o755); _ = os.MkdirAll("filtered",0o755)
	src:=filepath.Join("uploads",hdr.Filename)
	if err:=save(fh,src);err!=nil{http.Error(w,err.Error(),500);return}

	filtered,summary,err:=normBSNL(src,crime)
	if err!=nil{http.Error(w,err.Error(),500);return}

	fmt.Fprintf(w,"/download/%s\n/download/%s\n",filepath.Base(filtered),filepath.Base(summary))
}
func save(r io.Reader,dst string) error { f,err:=os.Create(dst); if err!=nil{return err}; defer f.Close();_,err=io.Copy(f,r);return err }

/* ─────────────── BSNL normaliser (filtered + summary) ─────────────── */
func normBSNL(src,crime string)(filteredPath,summaryPath string,err error){
	in,err:=os.Open(src); if err!=nil{return "", "", err}; defer in.Close()
	r:=csv.NewReader(in)

	/* ---- locate header + CDR ---- */
	var header []string
	var cdr string
	for{
		rec,er:=r.Read()
		if er==io.EOF{ return "", "", errors.New("no header") }
		if er!=nil{ continue }
		if cdr==""{ cdr=extractCDR(strings.Join(rec," ")) }
		if colIdx(rec,"call_date")!=-1{ header=rec; break }
	}
	firstData,er:=r.Read(); if er!=nil{ return "", "", errors.New("no data rows") }
	if cdr==""{
		if idx:=colIdxAny(header,"search value"); idx!=-1 && idx<len(firstData){
			cdr = digits(firstData[idx])
		}
	}
	if cdr==""{ cdr = digits(filepath.Base(src)) }
	if cdr==""{ return "","",errors.New("cannot determine CDR") }

	/* ---- column indexes ---- */
	idxDate := colIdx(header,"call_date")
	idxTime := colIdxAny(header,"call_initiation_time","cit")
	idxDur  := colIdx(header,"call_duration")
	idxB    := colIdx(header,"other_party_no")
	idxType := colIdx(header,"call_type")
	idxFid  := colIdx(header,"first_cell_id")
	idxLid  := colIdx(header,"last_cell_id")
	idxLaddr:= colIdx(header,"last_cell_desc")
	idxIMEI := colIdx(header,"imei")
	idxIMSI := colIdx(header,"imsi")
	idxRoam := colIdxAny(header,"roaming circle","roaming_circle")
	idxLRN  := colIdx(header,"lrn_b_party_no")
	idxSrv  := colIdx(header,"service_type")

	/* ---- filtered writer ---- */
	filteredPath = filepath.Join("filtered",cdr+"_reports.csv")
	fout,_:=os.Create(filteredPath); defer fout.Close()
	fw:=csv.NewWriter(fout); fw.Write(targetHeader)
	col:=map[string]int{}; for i,h:=range targetHeader{col[h]=i}
	blank:=make([]string,len(targetHeader))

	/* ---- summary aggregators ---- */
	type agg struct{
		BParty,Provider,SDR,Type string
		Tot,Out,In,OutSMS,InSMS,Other,RoamCall,RoamSMS int
		Dur float64
		Days,Cells,Imeis,Imsis map[string]struct{}
		First,Last string
	}
	summary:=map[string]*agg{}
	parseDT:=func(d,t string)string{ return strings.TrimSpace(d)+" "+strings.TrimSpace(t) }

	cp:=func(rec []string,src int,dst string,row []string){
		if src!=-1&&src<len(rec){ row[col[dst]]=strings.Trim(rec[src],"'\" ") }
	}

	writeRow:=func(rec []string){
		if len(rec)==0{ return }
		row:=append([]string(nil),blank...)
		row[col["CdrNo"]]=cdr; row[col["Crime"]]=crime
		cp(rec,idxDate,"Date",row); cp(rec,idxTime,"Time",row); cp(rec,idxDur,"Duration",row)
		cp(rec,idxB,"B Party",row);  cp(rec,idxType,"Call Type",row)
		cp(rec,idxFid,"First Cell ID",row); cp(rec,idxLid,"Last Cell ID",row)
		cp(rec,idxLaddr,"Last Cell ID Address",row)
		cp(rec,idxIMEI,"IMEI",row); cp(rec,idxIMSI,"IMSI",row)
		cp(rec,idxRoam,"Roaming",row); cp(rec,idxLRN,"LRN",row); cp(rec,idxSrv,"Type",row)

		if id:=pick(rec,idxFid); id!=""{ if info,ok:=cellLookup(id);ok{
			row[col["First Cell ID Address"]]=info.Addr
			row[col["Main City(First CellID)"]]=info.Main
			row[col["Sub City (First CellID)"]]=info.Sub
			row[col["Lat-Long-Azimuth (First CellID)"]]=info.LatLonAz
		}}

		if l:=digits(row[col["LRN"]]); l!=""{ if info,ok:=lrnDB[l];ok{
			row[col["B Party Provider"]]=info.Provider
			row[col["B Party Circle"]]=info.Circle
			row[col["B Party Operator"]]=info.Operator
		}}

		fw.Write(row)

		/* ---- summary ---- */
		bKey:=row[col["B Party"]]; if bKey==""{ bKey="(blank)" }
		a,ok:=summary[bKey]; if !ok{
			a=&agg{BParty:bKey,Provider:row[col["B Party Provider"]],
				SDR:row[col["B Party Operator"]],Type:row[col["Type"]],
				Days:map[string]struct{}{},Cells:map[string]struct{}{},
				Imeis:map[string]struct{}{},Imsis:map[string]struct{}{}}
			summary[bKey]=a
		}
		a.Tot++
		switch strings.ToUpper(row[col["Call Type"]]){
		case "CALL_OUT","O","OUT": a.Out++
		case "CALL_IN","I","IN":  a.In++
		default:
			if strings.Contains(row[col["Call Type"]],"SMS"){
				if strings.Contains(row[col["Call Type"]],"OUT"){ a.OutSMS++ } else { a.InSMS++ }
			}else{ a.Other++ }
		}
		if row[col["Roaming"]]!=""{
			if strings.Contains(row[col["Call Type"]],"SMS"){ a.RoamSMS++ } else { a.RoamCall++ }
		}
		if d,er:=strconv.ParseFloat(row[col["Duration"]],64);er==nil{ a.Dur+=d }
		a.Days[row[col["Date"]]]=struct{}{}
		if id:=row[col["First Cell ID"]]; id!=""{ a.Cells[id]=struct{}{} }
		if id:=row[col["Last Cell ID"]];  id!=""{ a.Cells[id]=struct{}{} }
		if v:=row[col["IMEI"]];v!=""{ a.Imeis[v]=struct{}{} }
		if v:=row[col["IMSI"]];v!=""{ a.Imsis[v]=struct{}{} }
		dt:=parseDT(row[col["Date"]],row[col["Time"]])
		if a.First==""||dt<a.First{ a.First=dt }
		if a.Last==""||dt>a.Last{ a.Last=dt }
	}

	/* feed rows */
	writeRow(firstData)
	for{
		rec,er:=r.Read(); if er==io.EOF{break}; if er!=nil||len(rec)==0{continue}
		writeRow(rec)
	}
	fw.Flush()

	/* summary file */
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
			fmt.Sprint(a.Tot),fmt.Sprint(a.Out),fmt.Sprint(a.In),
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
