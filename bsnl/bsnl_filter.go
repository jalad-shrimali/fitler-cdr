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
	"sort"
	"strconv"
	"strings"
	"time"
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
func norm(s string) string  { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }
func digits(s string) string{ return nonDigit.ReplaceAllString(s, "") }

/* header index helpers */
func colIdxAny(h []string, keys ...string) int { for _,k:=range keys{if i:=colIdx(h,k);i!=-1{return i}};return -1 }
func colIdx(h []string,key string) int { key=norm(key); for i,x:=range h{ if norm(x)==key { return i } }; return -1 }

/* banner extractor */
var searchValRE = regexp.MustCompile(`(?i)search\s*value[^0-9]*([0-9]{8,15})`)
func extractCDR(line string) string { if m:=searchValRE.FindStringSubmatch(line);len(m)>1{return m[1]};return"" }

/* ───────── embedded data ───────── */
//go:embed data/*
var dataFS embed.FS
type CellInfo struct{ Addr, Sub, Main, Lat, Lon, Az string }
type LRNInfo  struct{ Provider, Circle, Operator string }

var (
	cellDB = map[string]CellInfo{}  // id → info
	lrnDB  = map[string]LRNInfo{}   // digits(lrn) → info
)

func init() { loadCells("data/bsnl_cells.csv"); loadLRN("data/LRN.csv") }

/* ---------- loadCells ---------- */
func loadCells(path string){
	f,err:=dataFS.Open(path); if err!=nil{log.Printf("warning: %v",err);return}
	defer f.Close()
	r:=csv.NewReader(f); hdr,_:=r.Read()
	iID:=colIdxAny(hdr,"cgi","cell id","cell_id")
	iAddr:=colIdxAny(hdr,"address"); iSub:=colIdxAny(hdr,"subcity")
	iMain:=colIdxAny(hdr,"maincity","city")
	iLat:=colIdxAny(hdr,"latitude"); iLon:=colIdxAny(hdr,"longitude","lon")
	iAz:=colIdxAny(hdr,"azimuth","az")
	if iID==-1{log.Printf("warning: no CGI column in %s",path);return}
	for{
		rec,er:=r.Read(); if er==io.EOF{break}; if er!=nil||len(rec)==0{continue}
		raw:=strings.TrimSpace(rec[iID]); if raw==""{continue}
		info:=CellInfo{
			Addr: pick(rec,iAddr), Sub: pick(rec,iSub), Main: pick(rec,iMain),
			Lat:  pick(rec,iLat),  Lon: pick(rec,iLon),  Az:  pick(rec,iAz),
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
	if iLRN==-1||iTSP==-1{log.Printf("warning: incomplete LRN.csv");return}
	for{
		rec,er:=r.Read(); if er==io.EOF{break}; if er!=nil||len(rec)==0{continue}
		key:=digits(rec[iLRN]); if key==""{continue}
		lrnDB[key]=LRNInfo{Provider:rec[iTSP],Circle:pick(rec,iCircle),Operator:rec[iTSP]}
	}
}

/* small utilities */
func pick(rec []string,idx int)string{ if idx==-1||idx>=len(rec){return""}; return strings.TrimSpace(rec[idx]) }
func cellLookup(id string)(CellInfo,bool){
	if info,ok:=cellDB[id];ok{return info,true}
	if info,ok:=cellDB[digits(id)];ok{return info,true}
	return CellInfo{},false
}
func nonEmpty(s string)string{ if strings.TrimSpace(s)==""{return"Unknown"}; return s }

/* ───────────────── HTTP handler ───────────────── */
func UploadAndNormalizeCSV(w http.ResponseWriter,r *http.Request){
	if r.Method!=http.MethodPost{http.Error(w,"POST only",405);return}
	if strings.ToLower(r.FormValue("tsp_type"))!="bsnl"{http.Error(w,"Only BSNL supported",400);return}
	crime:=r.FormValue("crime_number")

	fh,hdr,err:=r.FormFile("file"); if err!=nil{http.Error(w,err.Error(),400);return}
	defer fh.Close()
	_ = os.MkdirAll("uploads",0o755); _ = os.MkdirAll("filtered",0o755)
	src:=filepath.Join("uploads",hdr.Filename)
	if err:=save(fh,src);err!=nil{http.Error(w,err.Error(),500);return}

	filtered,summary,maxCalls,maxDur,maxStay,err:=normBSNL(src,crime)
	if err!=nil{http.Error(w,err.Error(),500);return}
	fmt.Fprintf(w,
		"/download/%s\n/download/%s\n/download/%s\n/download/%s\n/download/%s\n",
		filepath.Base(filtered),filepath.Base(summary),
		filepath.Base(maxCalls),filepath.Base(maxDur),filepath.Base(maxStay))
}
func save(r io.Reader,dst string)error{f,err:=os.Create(dst);if err!=nil{return err};defer f.Close();_,err=io.Copy(f,r);return err}

/* ─────────── BSNL normaliser ─────────── */
func normBSNL(src,crime string)(filteredP,summaryP,maxCallsP,maxDurP,maxStayP string,err error){

	in,err:=os.Open(src); if err!=nil{return}; defer in.Close()
	r:=csv.NewReader(in)

	/* locate header + CDR */
	var header []string; var cdr string
	for{
		rec,er:=r.Read(); if er==io.EOF{err=errors.New("no header");return}
		if er!=nil{continue}
		if cdr==""{ cdr=extractCDR(strings.Join(rec," ")) }
		if colIdx(rec,"call_date")!=-1{ header=rec; break }
	}
	firstData,er:=r.Read(); if er!=nil{err=errors.New("header only");return}
	if cdr==""{
		if idx:=colIdxAny(header,"search value"); idx!=-1&&idx<len(firstData){
			cdr=digits(firstData[idx])
		}
	}
	if cdr==""{ cdr=digits(filepath.Base(src)) }
	if cdr==""{ err=errors.New("cannot find CDR"); return }

	/* indexes */
	iDate:=colIdx(header,"call_date")
	iTime:=colIdxAny(header,"call_initiation_time","cit")
	iDur :=colIdx(header,"call_duration")
	iB   :=colIdx(header,"other_party_no")
	iType:=colIdx(header,"call_type")
	iFid :=colIdx(header,"first_cell_id")
	iLid :=colIdx(header,"last_cell_id")
	iLaddr:=colIdx(header,"last_cell_desc")
	iIMEI:=colIdx(header,"imei")
	iIMSI:=colIdx(header,"imsi")
	iRoam:=colIdxAny(header,"roaming circle","roaming_circle")
	iLRN :=colIdx(header,"lrn_b_party_no")
	iSrv :=colIdx(header,"service_type")

	/* filtered writer */
	filteredP = filepath.Join("filtered",cdr+"_reports.csv")
	fout,_:=os.Create(filteredP); defer fout.Close()
	fw:=csv.NewWriter(fout); fw.Write(targetHeader)
	col:=map[string]int{}; for i,h:=range targetHeader{col[h]=i}
	blank:=make([]string,len(targetHeader))

	/* aggregators ------------------------------------------------------ */
	type partyAgg struct{ Provider string; Calls int; Dur float64 }
	parties:=map[string]*partyAgg{}
	totalCalls:=0; totalDur:=0.0

	type cellAgg struct{
		Addr,Lat,Lon,Az,Roam string
		Calls int
		First,Last string
	}
	cells:=map[string]*cellAgg{}
	parseDT:=func(d,t string)string{ return strings.TrimSpace(d)+" "+strings.TrimSpace(t) }

	cp:=func(rec []string,src int,dst string,row []string){
		if src!=-1&&src<len(rec){ row[col[dst]]=strings.Trim(rec[src],"'\" ") }
	}

	writeRow:=func(rec []string){
		if len(rec)==0{ return }
		row:=append([]string(nil),blank...)
		row[col["CdrNo"]]=cdr; row[col["Crime"]]=crime
		cp(rec,iDate,"Date",row); cp(rec,iTime,"Time",row); cp(rec,iDur,"Duration",row)
		cp(rec,iB,"B Party",row);  cp(rec,iType,"Call Type",row)
		cp(rec,iFid,"First Cell ID",row); cp(rec,iLid,"Last Cell ID",row)
		cp(rec,iLaddr,"Last Cell ID Address",row)
		cp(rec,iIMEI,"IMEI",row); cp(rec,iIMSI,"IMSI",row)
		cp(rec,iRoam,"Roaming",row); cp(rec,iLRN,"LRN",row); cp(rec,iSrv,"Type",row)

		/* cell enrichment (first) */
		if id:=pick(rec,iFid);id!=""{ if info,ok:=cellLookup(id);ok{
			row[col["First Cell ID Address"]]=info.Addr
			row[col["Main City(First CellID)"]]=info.Main
			row[col["Sub City (First CellID)"]]=info.Sub
			row[col["Lat-Long-Azimuth (First CellID)"]]=info.Lat+","+info.Lon+","+info.Az
		}}

		/* LRN enrichment -> provider */
		if l:=digits(row[col["LRN"]]); l!=""{ if info,ok:=lrnDB[l]; ok{
			row[col["B Party Provider"]]=info.Provider
			row[col["B Party Circle"]]=info.Circle
			row[col["B Party Operator"]]=info.Operator
		}}
		if row[col["B Party Provider"]]==""&&strings.Contains(strings.ToUpper(row[col["B Party"]]),"BSNL"){
			row[col["B Party Provider"]]="BSNL"
		}
		fw.Write(row)

		/* --- per‑party accumulation */
		bKey:=row[col["B Party"]]; if bKey==""{ bKey="(blank)" }
		if _,ok:=parties[bKey]; !ok { parties[bKey]=&partyAgg{} }
		pa:=parties[bKey]
		if p:=row[col["B Party Provider"]]; p!=""{ pa.Provider=p }
		pa.Calls++
		if d,er:=strconv.ParseFloat(row[col["Duration"]],64);er==nil{ pa.Dur+=d }
		totalCalls++
		if d,er:=strconv.ParseFloat(row[col["Duration"]],64);er==nil{ totalDur+=d }

		/* --- per‑cell accumulation (first cell) */
		cid:=row[col["First Cell ID"]]
		if cid!=""{
			if _,ok:=cells[cid];!ok{ cells[cid]=&cellAgg{} }
			ca:=cells[cid]
			if info,ok:=cellLookup(cid); ok && ca.Addr==""{
				ca.Addr=info.Addr; ca.Lat=info.Lat; ca.Lon=info.Lon; ca.Az=info.Az
			}
			if ca.Roam==""{ ca.Roam=row[col["Roaming"]] }
			ca.Calls++
			dt:=parseDT(row[col["Date"]],row[col["Time"]])
			if ca.First==""||dt<ca.First{ ca.First=dt }
			if ca.Last==""||dt>ca.Last{ ca.Last=dt }
		}
	}
	writeRow(firstData)
	for{ rec,er:=r.Read(); if er==io.EOF{break}; if er!=nil||len(rec)==0{continue}; writeRow(rec) }
	fw.Flush()

	/* summary file (unchanged‑simple) */
	summaryP = filepath.Join("filtered",cdr+"_summary_reports.csv")
	sout,_:=os.Create(summaryP); defer sout.Close()
	sw:=csv.NewWriter(sout)
	sw.Write([]string{"CdrNo","B Party","B Party SDR","Provider","Total Calls","Total Duration"})
	for b,a:=range parties{
		sw.Write([]string{cdr,b,"",nonEmpty(a.Provider),fmt.Sprint(a.Calls),fmt.Sprintf("%.0f",a.Dur)})
	}
	sw.Flush()

	/* max‑calls report */
	type kvCalls struct{ Party string; *partyAgg }
	var list []kvCalls
	for p,a:=range parties{ list=append(list,kvCalls{p,a}) }
	sort.Slice(list,func(i,j int)bool{ return list[i].Calls>list[j].Calls })
	maxCallsP = filepath.Join("filtered",cdr+"_max_calls_report.csv")
	wc,_:=os.Create(maxCallsP); mw:=csv.NewWriter(wc)
	mw.Write([]string{"CdrNo","B Party","B Party SDR","Total Calls","Provider"})
	topProv:="Unknown"; if len(list)>0{ topProv=nonEmpty(list[0].Provider) }
	mw.Write([]string{"Total",cdr,"",fmt.Sprint(totalCalls),topProv})
	for _,v:=range list{
		mw.Write([]string{cdr,v.Party,"",fmt.Sprint(v.Calls),nonEmpty(v.Provider)})
	}
	mw.Flush(); wc.Close()

	/* max‑duration report */
	sort.Slice(list,func(i,j int)bool{ return list[i].Dur>list[j].Dur })
	maxDurP = filepath.Join("filtered",cdr+"_max_duration_report.csv")
	wd,_:=os.Create(maxDurP); md:=csv.NewWriter(wd)
	md.Write([]string{"CdrNo","B Party","B Party SDR","Total Duration","Provider"})
	for _,v:=range list{
		md.Write([]string{cdr,v.Party,"",fmt.Sprintf("%.0f",v.Dur),nonEmpty(v.Provider)})
	}
	md.Flush(); wd.Close()

	/* max‑stay report */
	type cellkv struct{ ID string; *cellAgg }
	var clist []cellkv
	for id,c:=range cells{ clist=append(clist,cellkv{id,c}) }
	sort.Slice(clist,func(i,j int)bool{ return clist[i].Calls>clist[j].Calls })
	maxStayP = filepath.Join("filtered",cdr+"_max_stay_report.csv")
	ws,_:=os.Create(maxStayP); st:=csv.NewWriter(ws)
	st.Write([]string{
		"CdrNo","Cell ID","Total Calls","Tower Address",
		"Latitude","Longitude","Azimuth","Roaming","First Call","Last Call",
	})
	for _,c:=range clist{
		st.Write([]string{
			cdr,c.ID,fmt.Sprint(c.Calls),c.Addr,c.Lat,c.Lon,c.Az,
			nonEmpty(c.Roam),formatDT(c.First),formatDT(c.Last),
		})
	}
	st.Flush(); ws.Close()

	return filteredP,summaryP,maxCallsP,maxDurP,maxStayP,nil
}

func formatDT(dt string)string{
	if dt==""{return""}
	t,err:=time.Parse("2006-01-02 15:04:05",dt)
	if err!=nil{ return dt }
	return t.Format("02-Jan-2006 15:04:05")
}
