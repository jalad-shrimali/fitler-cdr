package main

import (
	"log"
	"net/http"

)

func main() {
	http.HandleFunc("/upload", UploadAndNormalizeCSV)
	http.Handle("/download/",
		http.StripPrefix("/download/", http.FileServer(http.Dir("filtered"))))

	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// package main

// import (
// 	"log"
// 	"net/http"
// 	"strings"

// 	"github.com/jalad-shrimali/cdr-filter/vi"
// 	"github.com/jalad-shrimali/cdr-filter/bsnl"
// 	"github.com/jalad-shrimali/cdr-filter/jio"
// 	"github.com/jalad-shrimali/cdr-filter/airtel"
// )

// // central dispatcher
// func uploadHandler(w http.ResponseWriter, r *http.Request) {
// 	tsp := strings.ToLower(r.FormValue("tsp_type"))
// 	switch tsp {
// 	case "jio":
// 		jio.UploadAndNormalizeCSV(w, r)
// 	case "vi":
// 		vi.UploadAndNormalizeCSV(w, r)
// 	case "bsnl":
// 		bsnl.UploadAndNormalizeCSV(w, r)
// 	case "airtel":
// 		airtel.UploadAndNormalizeCSV(w, r)
// 	default:
// 		http.Error(w, "unknown or missing tsp_type", http.StatusBadRequest)
// 	}
// }

// func main() {
// 	http.HandleFunc("/upload", uploadHandler)

// 	// static file download
// 	http.Handle("/download/",
// 		http.StripPrefix("/download/",
// 			http.FileServer(http.Dir("filtered"))))

// 	log.Println("Server started on :8080")
// 	log.Fatal(http.ListenAndServe(":8080", nil))
// }
