// package handlers

// import (
// 	"encoding/csv"
// 	"fmt"
// 	"io"
// 	"net/http"
// 	"os"
// 	"path/filepath"
// 	"regexp"
// 	"strings"
// )

// /* ──────────── canonical 26-column layout (keep order) ──────────────── */

// var targetHeader = []string{
// 	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
// 	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
// 	"IMEI", "IMSI", "Roaming", "Main City(First CellID)", "Sub City (First CellID)",
// 	"Lat-Long-Azimuth (First CellID)", "Crime", "Circle", "Operator", "LRN",
// 	"CallForward", "B Party Provider", "B Party Circle", "B Party Operator",
// 	"Type", "IMEI Manufacturer",
// }

// /* ──────────── column-name synonyms (trim-and-lowered) ──────────────── */

// var synonyms = map[string]string{
// 	// numbers
// 	// "target no": "CdrNo", "calling number": "CdrNo",
// 	// "calling party telephone number": "CdrNo",
// 	"b party no": "B Party", "called party telephone number": "B Party",

// 	// date / time / duration
// 	"date": "Date", "call date": "Date",
// 	"time": "Time", "call time": "Time",
// 	"dur(s)": "Duration", "call duration": "Duration",

// 	// call type
// 	"call type": "Call Type",

// 	// device / subscriber
// 	"imei": "IMEI", "imsi": "IMSI",

// 	// roaming / operator
// 	"roam nw": "Roaming", "roaming circle name": "Circle",
// 	"circle": "Circle", "operator": "Operator",

// 	// lrn / forwarding
// 	"lrn": "LRN", "lrn called no": "LRN",
// 	"call fow no": "CallForward", "call forwarding": "CallForward",

// 	// provider
// 	"lrn tsp-lsa": "B Party Provider", "b party provider": "B Party Provider",
// 	"b party circle": "B Party Circle", "b party operator": "B Party Operator",

// 	// misc
// 	"service type": "Type",
// 	"crime": "Crime",
// }

// /* ───────────── helpers ───────────── */

// var spaceRE = regexp.MustCompile(`\s+`)

// func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }

// // Regular expressions for CDR number extraction
// var (
// 	airtelCdrRE = regexp.MustCompile(`Mobile No '(\d+)'`)
// 	jioCdrRE    = regexp.MustCompile(`Input Value : (\d+)`)
// 	viCdrRE     = regexp.MustCompile(`MSISDN : - (\d+)`)
// )

// // Helper function to extract CDR number based on TSP type
// func extractCdrNumber(tsp, content string) string {
// 	switch strings.ToLower(tsp) {
// 	case "airtel":
// 		matches := airtelCdrRE.FindStringSubmatch(content)
// 		if len(matches) > 1 {
// 			return matches[1]
// 		}
// 	case "jio":
// 		matches := jioCdrRE.FindStringSubmatch(content)
// 		if len(matches) > 1 {
// 			return matches[1]
// 		}
// 	case "vi":
// 		matches := viCdrRE.FindStringSubmatch(content)
// 		if len(matches) > 1 {
// 			return matches[1]
// 		}
// 	}
// 	return ""
// }

// /* ───────────── HTTP endpoint ───────────── */

// func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodPost {
// 		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
// 		return
// 	}
// 	tsp := strings.ToLower(r.FormValue("tsp_type"))
// 	crime := r.FormValue("crime_number")

// 	file, hdr, err := r.FormFile("file")
// 	if err != nil { http.Error(w, err.Error(), 400); return }
// 	defer file.Close()

// 	for _, d := range []string{"uploads", "filtered"} {
// 		if err := os.MkdirAll(d, 0755); err != nil {
// 			http.Error(w, err.Error(), 500); return
// 		}
// 	}
// 	src := filepath.Join("uploads", hdr.Filename)
// 	dst := filepath.Join("filtered", "filtered_"+hdr.Filename)
// 	if err := saveUploaded(file, src); err != nil {
// 		http.Error(w, err.Error(), 500); return
// 	}

// 	switch tsp {
// 	case "airtel":
// 		if err := normalizeAirtel(src, dst, crime, tsp); err != nil {
// 			http.Error(w, "normalization failed: "+err.Error(), 500)
// 			return
// 		}
// 	case "jio", "vi":
// 		// Add similar normalization functions for Jio and Vi when needed
// 		http.Error(w, "TSP type not yet implemented", 400)
// 		return
// 	default:
// 		http.Error(w, "unsupported tsp_type", 400)
// 		return
// 	}

// 	fmt.Fprintf(w, "Normalized file created: /download/%s\n", filepath.Base(dst))
// }

// func saveUploaded(src io.Reader, dst string) error {
// 	f, err := os.Create(dst); if err != nil { return err }
// 	defer f.Close()
// 	_, err = io.Copy(f, src)
// 	return err
// }

// /* ───────────── Airtel normalizer ───────────── */

// // cleanCGI function to remove hyphens only
// func cleanCGI(raw string) string {
// 	// Remove hyphens from the input string
// 	raw = strings.ReplaceAll(raw, "-", "")
// 	return raw
// }

// func normalizeAirtel(src, dst, crime, tsp string) error {
// 	in, err := os.Open(src)
// 	if err != nil {
// 		return err
// 	}
// 	defer in.Close()
// 	r := csv.NewReader(in)

// 	// Find header row and CDR number
// 	var header []string
// 	var cdrNumber string
// 	for {
// 		rec, err := r.Read()
// 		if err == io.EOF {
// 			return fmt.Errorf("no header found")
// 		}
// 		if err != nil {
// 			continue
// 		}

// 		// Look for CDR number in description lines
// 		if cdrNumber == "" && len(rec) > 0 {
// 			cdrNumber = extractCdrNumber(tsp, rec[0])
// 		}

// 		// Look for specific Airtel header markers
// 		if len(rec) > 0 && strings.Contains(rec[0], "Target No") {
// 			header = rec
// 			break
// 		}
// 	}

// 	if cdrNumber == "" {
// 		return fmt.Errorf("could not extract CDR number from file")
// 	}

// 	// Build column mappings
// 	srcToDst := map[int]int{}
// 	c2src, c2dst := map[string]int{}, map[string]int{}

// 	// First explicitly find the CGI columns
// 	var firstCGIIndex, lastCGIIndex = -1, -1
// 	for i, h := range header {
// 		h = norm(h)
// 		if h == "first cgi" {
// 			firstCGIIndex = i
// 		}
// 		if h == "last cgi" {
// 			lastCGIIndex = i
// 		}
// 	}

// 	if firstCGIIndex == -1 || lastCGIIndex == -1 {
// 		return fmt.Errorf("CSV lacks First/Last CGI columns")
// 	}

// 	// Map other columns
// 	for i, h := range header {
// 		if canon, ok := synonyms[norm(h)]; ok {
// 			for j, want := range targetHeader {
// 				if want == canon {
// 					srcToDst[i] = j
// 					c2src[canon] = i
// 					c2dst[canon] = j
// 					break
// 				}
// 			}
// 		}
// 	}

// 	// Force mapping of CGI columns
// 	for j, want := range targetHeader {
// 		if want == "First Cell ID" {
// 			srcToDst[firstCGIIndex] = j
// 			c2src["First Cell ID"] = firstCGIIndex
// 			c2dst["First Cell ID"] = j
// 		}
// 		if want == "Last Cell ID" {
// 			srcToDst[lastCGIIndex] = j
// 			c2src["Last Cell ID"] = lastCGIIndex
// 			c2dst["Last Cell ID"] = j
// 		}
// 	}

// 	// Ensure Crime column exists
// 	if _, ok := c2dst["Crime"]; !ok {
// 		for i, name := range targetHeader {
// 			if name == "Crime" {
// 				c2dst["Crime"] = i
// 				break
// 			}
// 		}
// 	}

// 	// Load cell-ID mapping from the file
// 	mp, err := os.Open("data/test MP Airtel cell - test MP Airtel cell.csv")
// 	if err != nil {
// 		return err
// 	}
// 	defer mp.Close()
// 	mr := csv.NewReader(mp)
// 	head, _ := mr.Read()

// 	// Create a map for column headers
// 	col := map[string]int{}
// 	for i, h := range head {
// 		col[norm(h)] = i
// 	}
// 	req := []string{"cgi", "address", "maincity", "subcity", "latitude", "longitude", "azimuth"}
// 	for _, k := range req {
// 		if _, ok := col[k]; !ok {
// 			return fmt.Errorf("mapping missing %q", k)
// 		}
// 	}

// 	// Create a map to store the cell data
// 	cgiMap := map[string]map[string]string{}
// 	for {
// 		rec, err := mr.Read()
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			continue
// 		}
// 		key := cleanCGI(rec[col["cgi"]]) // Clean hyphens here before matching
// 		cgiMap[key] = map[string]string{
// 			"addr":   rec[col["address"]],
// 			"main":   rec[col["maincity"]],
// 			"sub":    rec[col["subcity"]],
// 			"lat":    rec[col["latitude"]],
// 			"lon":    rec[col["longitude"]],
// 			"az":     rec[col["azimuth"]],
// 		}
// 	}

// 	// Process records from the input CSV
// 	out, _ := os.Create(dst)
// 	defer out.Close()
// 	w := csv.NewWriter(out)
// 	_ = w.Write(targetHeader)
// 	blank := make([]string, len(targetHeader))

// 	skip := func(s string) bool {
// 		s = strings.ToLower(strings.TrimSpace(s))
// 		return strings.HasPrefix(s, "this is system") || strings.Contains(s, "system generated")
// 	}

// 	for {
// 		rec, err := r.Read()
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil || len(rec) == 0 || skip(rec[0]) {
// 			continue
// 		}

// 		// Initialize a new row for the output CSV
// 		row := append([]string(nil), blank...)

// 		// Set the extracted CDR number for all records
// 		row[c2dst["CdrNo"]] = cdrNumber

// 		// Copy normal columns from source to destination
// 		for s, d := range srcToDst {
// 			if s >= len(rec) {
// 				continue
// 			}
// 			v := strings.Trim(rec[s], "'\" ")
// 			switch targetHeader[d] {
// 			case "Call Type":
// 				vU := strings.ToUpper(v)
// 				if vU == "IN" || vU == "A_IN" {
// 					v = "CALL_IN"
// 				}
// 				if vU == "OUT" || vU == "A_OUT" {
// 					v = "CALL_OUT"
// 				}
// 			case "Type":
// 				if strings.EqualFold(v, "pre") {
// 					v = "Prepaid"
// 				}
// 				if strings.EqualFold(v, "post") {
// 					v = "Postpaid"
// 				}
// 			}
// 			row[d] = v
// 		}

// 		// Write crime number
// 		row[c2dst["Crime"]] = crime

// 		// Clean First Cell ID and map
// 		if firstCGI := rec[firstCGIIndex]; firstCGI != "" {
// 			cleanedFirstCGI := cleanCGI(firstCGI) // Clean hyphens here before matching
// 			if m := cgiMap[cleanedFirstCGI]; m != nil {
// 				row[c2dst["First Cell ID Address"]] = m["addr"]
// 				row[c2dst["Main City(First CellID)"]] = m["main"]
// 				row[c2dst["Sub City (First CellID)"]] = m["sub"]
// 				row[c2dst["Lat-Long-Azimuth (First CellID)"]] = fmt.Sprintf("%s,%s,%s", m["lat"], m["lon"], m["az"])
// 				// Write the cleaned First Cell ID (no hyphens)
// 				row[c2dst["First Cell ID"]] = strings.ReplaceAll(firstCGI, "-", "") // Save without hyphens
// 			}
// 		}

// 		// Clean Last Cell ID and map
// 		if lastCGI := rec[lastCGIIndex]; lastCGI != "" {
// 			cleanedLastCGI := cleanCGI(lastCGI) // Clean hyphens here before matching
// 			if m := cgiMap[cleanedLastCGI]; m != nil {
// 				row[c2dst["Last Cell ID Address"]] = m["addr"]
// 				// Write the cleaned Last Cell ID (no hyphens)
// 				row[c2dst["Last Cell ID"]] = strings.ReplaceAll(lastCGI, "-", "") // Save without hyphens
// 			}
// 		}

// 		// Write the row to the output CSV
// 		if err := w.Write(row); err != nil {
// 			return err
// 		}
// 	}
// 	w.Flush()
// 	return w.Error()
// }



package handlers

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

/* ──────────── canonical 26-column layout (keep order) ──────────────── */

var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming", "Main City(First CellID)", "Sub City (First CellID)",
	"Lat-Long-Azimuth (First CellID)", "Crime", "Circle", "Operator", "LRN",
	"CallForward", "B Party Provider", "B Party Circle", "B Party Operator",
	"Type", "IMEI Manufacturer",
}

/* ──────────── column-name synonyms (trim-and-lowered) ──────────────── */

var synonyms = map[string]string{
	"b party no": "B Party", "called party telephone number": "B Party",
	"date": "Date", "call date": "Date",
	"time": "Time", "call time": "Time",
	"dur(s)": "Duration", "call duration": "Duration",
	"call type": "Call Type",
	"imei": "IMEI", "imsi": "IMSI",
	"roam nw": "Roaming", "roaming circle name": "Circle",
	"circle": "Circle", "operator": "Operator",
	"lrn": "LRN", "lrn called no": "LRN",
	"call fow no": "CallForward", "call forwarding": "CallForward",
	"lrn tsp-lsa": "B Party Provider", "b party provider": "B Party Provider",
	"b party circle": "B Party Circle", "b party operator": "B Party Operator",
	"service type": "Type",
	"crime": "Crime",
}

/* ───────────── helpers ───────────── */

var spaceRE = regexp.MustCompile(`\s+`)

func norm(s string) string { return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ") }

// Regular expressions for CDR number extraction
var (
	airtelCdrRE = regexp.MustCompile(`Mobile No '(\d+)'`)
	jioCdrRE    = regexp.MustCompile(`Input Value : (\d+)`)
	viCdrRE     = regexp.MustCompile(`MSISDN : - (\d+)`)
)

// Helper function to extract CDR number based on TSP type
func extractCdrNumber(tsp, content string) string {
	switch strings.ToLower(tsp) {
	case "airtel":
		matches := airtelCdrRE.FindStringSubmatch(content)
		if len(matches) > 1 {
			return matches[1]
		}
	case "jio":
		matches := jioCdrRE.FindStringSubmatch(content)
		if len(matches) > 1 {
			return matches[1]
		}
	case "vi":
		matches := viCdrRE.FindStringSubmatch(content)
		if len(matches) > 1 {
			return matches[1]
		}
	}
	return ""
}

/* ───────────── HTTP endpoint ───────────── */

func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}
	tsp := strings.ToLower(r.FormValue("tsp_type"))
	crime := r.FormValue("crime_number")

	file, hdr, err := r.FormFile("file")
	if err != nil { http.Error(w, err.Error(), 400); return }
	defer file.Close()

	for _, d := range []string{"uploads", "filtered"} {
		if err := os.MkdirAll(d, 0755); err != nil {
			http.Error(w, err.Error(), 500); return
		}
	}
	src := filepath.Join("uploads", hdr.Filename)
	dst := filepath.Join("filtered", "filtered_"+hdr.Filename)
	if err := saveUploaded(file, src); err != nil {
		http.Error(w, err.Error(), 500); return
	}

	switch tsp {
	case "airtel":
		if err := normalizeAirtel(src, dst, crime, tsp); err != nil {
			http.Error(w, "normalization failed: "+err.Error(), 500)
			return
		}
	case "jio", "vi":
		// Add similar normalization functions for Jio and Vi when needed
		http.Error(w, "TSP type not yet implemented", 400)
		return
	default:
		http.Error(w, "unsupported tsp_type", 400)
		return
	}

	fmt.Fprintf(w, "Normalized file created: /download/%s\n", filepath.Base(dst))
}

func saveUploaded(src io.Reader, dst string) error {
	f, err := os.Create(dst); if err != nil { return err }
	defer f.Close()
	_, err = io.Copy(f, src)
	return err
}

/* ───────────── Airtel normalizer ───────────── */

// cleanCGI function to remove hyphens only
func cleanCGI(raw string) string {
	// Remove hyphens from the input string
	raw = strings.ReplaceAll(raw, "-", "")
	return raw
}

func normalizeAirtel(src, dst, crime, tsp string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	r := csv.NewReader(in)

	// Find header row and CDR number
	var header []string
	var cdrNumber string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return fmt.Errorf("no header found")
		}
		if err != nil {
			continue
		}

		// Look for CDR number in description lines
		if cdrNumber == "" && len(rec) > 0 {
			cdrNumber = extractCdrNumber(tsp, rec[0])
		}

		// Look for specific Airtel header markers
		if len(rec) > 0 && strings.Contains(rec[0], "Target No") {
			header = rec
			break
		}
	}

	if cdrNumber == "" {
		return fmt.Errorf("could not extract CDR number from file")
	}

	// Build column mappings
	srcToDst := map[int]int{}
	c2src, c2dst := map[string]int{}, map[string]int{}

	// First explicitly find the CGI columns
	var firstCGIIndex, lastCGIIndex = -1, -1
	for i, h := range header {
		h = norm(h)
		if h == "first cgi" {
			firstCGIIndex = i
		}
		if h == "last cgi" {
			lastCGIIndex = i
		}
	}

	if firstCGIIndex == -1 || lastCGIIndex == -1 {
		return fmt.Errorf("CSV lacks First/Last CGI columns")
	}

	// Map other columns
	for i, h := range header {
		if canon, ok := synonyms[norm(h)]; ok {
			for j, want := range targetHeader {
				if want == canon {
					srcToDst[i] = j
					c2src[canon] = i
					c2dst[canon] = j
					break
				}
			}
		}
	}

	// Force mapping of CGI columns
	for j, want := range targetHeader {
		if want == "First Cell ID" {
			srcToDst[firstCGIIndex] = j
			c2src["First Cell ID"] = firstCGIIndex
			c2dst["First Cell ID"] = j
		}
		if want == "Last Cell ID" {
			srcToDst[lastCGIIndex] = j
			c2src["Last Cell ID"] = lastCGIIndex
			c2dst["Last Cell ID"] = j
		}
	}

	// Ensure Crime column exists
	if _, ok := c2dst["Crime"]; !ok {
		for i, name := range targetHeader {
			if name == "Crime" {
				c2dst["Crime"] = i
				break
			}
		}
	}

	// Process records from the input CSV
	out, _ := os.Create(dst)
	defer out.Close()
	w := csv.NewWriter(out)
	_ = w.Write(targetHeader)
	blank := make([]string, len(targetHeader))

	skip := func(s string) bool {
		s = strings.ToLower(strings.TrimSpace(s))
		return strings.HasPrefix(s, "this is system") || strings.Contains(s, "system generated")
	}

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) == 0 || skip(rec[0]) {
			continue
		}

		// Initialize a new row for the output CSV
		row := append([]string(nil), blank...)

		// Set the extracted CDR number for all records
		row[c2dst["CdrNo"]] = cdrNumber

		// Copy normal columns from source to destination
		for s, d := range srcToDst {
			if s >= len(rec) {
				continue
			}
			v := strings.Trim(rec[s], "'\" ")
			switch targetHeader[d] {
			case "Call Type":
				vU := strings.ToUpper(v)
				if vU == "IN" || vU == "A_IN" {
					v = "CALL_IN"
				}
				if vU == "OUT" || vU == "A_OUT" {
					v = "CALL_OUT"
				}
			case "Type":
				if strings.EqualFold(v, "pre") {
					v = "Prepaid"
				}
				if strings.EqualFold(v, "post") {
					v = "Postpaid"
				}
			}
			row[d] = v
		}

		// Write crime number
		row[c2dst["Crime"]] = crime

		// Clean First Cell ID and Last Cell ID and write them
		if firstCGI := rec[firstCGIIndex]; firstCGI != "" {
			row[c2dst["First Cell ID"]] = strings.ReplaceAll(firstCGI, "-", "")
		}
		if lastCGI := rec[lastCGIIndex]; lastCGI != "" {
			row[c2dst["Last Cell ID"]] = strings.ReplaceAll(lastCGI, "-", "")
		}

		// Write the row to the output CSV
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
