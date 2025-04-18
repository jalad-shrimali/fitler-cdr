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

/* ──────────────────────────── mapping tables ───────────────────────────── */

var targetHeader = []string{
	"CdrNo", "B Party", "Date", "Time", "Duration", "Call Type",
	"First Cell ID", "First Cell ID Address", "Last Cell ID", "Last Cell ID Address",
	"IMEI", "IMSI", "Roaming", "Main City(First CellID)", "Sub City (First CellID)",
	"Lat-Long-Azimuth (First CellID)", "Crime", "Circle", "Operator", "LRN",
	"CallForward", "B Party Provider", "B Party Circle", "B Party Operator",
	"Type", "IMEI Manufacturer",
}

// normalised source‑column → canonical 26‑column
var synonyms = map[string]string{
	// numbers
	"target no":                      "CdrNo",
	"calling party telephone number": "CdrNo",
	"calling number":                 "CdrNo",
	"b party no":                     "B Party",
	"called party telephone number":  "B Party",

	// date / time / duration
	"date":          "Date",
	"call date":     "Date",
	"time":          "Time",
	"call time":     "Time",
	"dur(s)":        "Duration",
	"call duration": "Duration",

	// call type
	"call type": "Call Type",

	// cell IDs
	"first cell id": "First Cell ID",
	"first cgi":     "First Cell ID",
	"last cell id":  "Last Cell ID",
	"last cgi":      "Last Cell ID",

	// device / subscriber
	"imei": "IMEI",
	"imsi": "IMSI",

	// roaming / operator
	"roam nw":             "Roaming",
	"roaming circle name": "Circle",
	"circle":              "Circle",
	"operator":            "Operator",

	// lrn / call‑forward
	"lrn no":          "LRN",
	"lrn called no":   "LRN",
	"lrn":             "LRN",
	"call fow no":     "CallForward",
	"call forwarding": "CallForward",

	// provider
	"lrn tsp-lsa":      "B Party Provider",
	"b party provider": "B Party Provider",
	"b party circle":   "B Party Circle",
	"b party operator": "B Party Operator",

	// misc
	"service type": "Type",
}

/* ───────────────────────── utility functions ───────────────────────────── */

var spaceRE = regexp.MustCompile(`\s+`)

func norm(s string) string { // trim, lower, collapse spaces
	return spaceRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), " ")
}

/* ──────────────────────────── main handler ─────────────────────────────── */

// UploadAndNormalizeCSV converts any operator CSV into the 26‑column format.
func UploadAndNormalizeCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	/* ---- receive the file ------------------------------------------------ */
	file, handler, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Error retrieving file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	for _, dir := range []string{"uploads", "filtered"} {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			http.Error(w, "Unable to create "+dir+": "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	srcPath := filepath.Join("uploads", handler.Filename)
	dstPath := filepath.Join("filtered", "filtered_"+handler.Filename)

	if err := saveUploaded(file, srcPath); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := normalizeFile(srcPath, dstPath); err != nil {
		http.Error(w, "Normalization failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Normalized file created: /download/%s\n", filepath.Base(dstPath))
}

/* ───────────────────── low‑level helpers (IO & transform) ─────────────── */

func saveUploaded(src io.Reader, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("save upload: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, src); err != nil {
		return fmt.Errorf("write upload: %w", err)
	}
	return nil
}

func normalizeFile(srcPath, dstPath string) error {
	inF, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer inF.Close()
	r := csv.NewReader(inF)

	srcHeader, err := r.Read()
	if err != nil {
		return err
	}

	// map src‑index → dst‑index
	indexMap := make(map[int]int)
	for i, h := range srcHeader {
		if canon, ok := synonyms[norm(h)]; ok {
			for j, want := range targetHeader {
				if want == canon {
					indexMap[i] = j
					break
				}
			}
		}
	}

	outF, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer outF.Close()
	w := csv.NewWriter(outF)

	if err := w.Write(targetHeader); err != nil {
		return err
	}

	blank := make([]string, len(targetHeader))

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Skipping malformed line:", err)
			continue
		}

		row := append([]string(nil), blank...) // fresh slice
		for srcIdx, dstIdx := range indexMap {
			if srcIdx < len(rec) {
				row[dstIdx] = strings.Trim(rec[srcIdx], "'") // remove stray quotes
			}
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
