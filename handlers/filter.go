package handlers

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// UploadAndFilterCSV reads an uploaded CSV and returns all rows whose
// <search_field> exactly matches <search_value> (case‑insensitive).
func UploadAndFilterCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	// ── 1. Get user parameters ────────────────────────────────────────────────
	fieldName := strings.TrimSpace(r.FormValue("search_field"))
	fieldValue := strings.TrimSpace(r.FormValue("search_value"))
	if fieldName == "" || fieldValue == "" {
		http.Error(w, "search_field and search_value are required", http.StatusBadRequest)
		return
	}

	// ── 2. Receive & persist the upload ───────────────────────────────────────
	file, handler, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Error retrieving file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	if err := os.MkdirAll("uploads", os.ModePerm); err != nil {
		http.Error(w, "Unable to create uploads dir: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.MkdirAll("filtered", os.ModePerm); err != nil {
		http.Error(w, "Unable to create filtered dir: "+err.Error(), http.StatusInternalServerError)
		return
	}

	uploadPath := filepath.Join("uploads", handler.Filename)
	out, err := os.Create(uploadPath)
	if err != nil {
		http.Error(w, "Unable to save uploaded file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := io.Copy(out, file); err != nil {
		out.Close()
		http.Error(w, "Failed to write uploaded file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	out.Close() // close before reopening

	// ── 3. Open for reading & set up writer for the result ────────────────────
	in, err := os.Open(uploadPath)
	if err != nil {
		http.Error(w, "Error opening uploaded file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer in.Close()

	reader := csv.NewReader(in)

	header, err := reader.Read()
	if err != nil {
		http.Error(w, "Error reading header: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Build a map of normalised header names → column index
	colIndex := make(map[string]int, len(header))
	for i, h := range header {
		colIndex[strings.ToLower(strings.TrimSpace(h))] = i
	}

	idx, ok := colIndex[strings.ToLower(fieldName)]
	if !ok {
		http.Error(w, fmt.Sprintf("Column %q not found in CSV", fieldName), http.StatusBadRequest)
		return
	}

	filteredPath := filepath.Join("filtered", "filtered_"+handler.Filename)
	outFiltered, err := os.Create(filteredPath)
	if err != nil {
		http.Error(w, "Unable to create filtered file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer outFiltered.Close()

	writer := csv.NewWriter(outFiltered)
	// copy the full header
	if err := writer.Write(header); err != nil {
		http.Error(w, "Error writing header: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// ── 4. Stream‑filter rows ────────────────────────────────────────────────
	matches := 0
	want := strings.ToLower(fieldValue)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// skip malformed line but keep the server alive
			fmt.Println("Skipping bad record:", err)
			continue
		}

		if strings.EqualFold(strings.TrimSpace(record[idx]), want) {
			if err := writer.Write(record); err != nil {
				fmt.Println("Error writing record:", err)
			}
			matches++
		}
	}
	writer.Flush()

	// ── 5. Respond ───────────────────────────────────────────────────────────
	if matches == 0 {
		fmt.Fprintf(w, "No matching records found for %s = %s\n", fieldName, fieldValue)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Filtered file created: /download/%s\n", filepath.Base(filteredPath))
}
