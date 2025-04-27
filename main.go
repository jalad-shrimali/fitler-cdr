package main

import (
	"log"
	"net/http"

	// "github.com/jalad-shrimali/cdr-filter/handlers"
)

func main() {
	http.HandleFunc("/upload", UploadAndNormalizeCSV)
	http.Handle("/download/",
		http.StripPrefix("/download/", http.FileServer(http.Dir("filtered"))))

	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
