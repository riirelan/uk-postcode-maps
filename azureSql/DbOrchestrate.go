/*
Created on Mon Jan 20 12:37:15 2020

@author: rich_ireland@hotmail.com
@version: 1.0
@versionDate: 2020-01-20

This python3 script
* Downloads all the UK postcode longitude,latitude from
      http://download.geonames.org/export/zip/GB_full.csv.zip
* Extracts the download
* Reads the resulting CSV of long/lats into a 2D array
* Optionally, 'grows' each point outwards to infil unallocated long/lats at
      a postcode district level
* Draws the resulting image with randomly assigned postcode district colouring
* Saves the image as BMP

History
Date         Version         Action
2020-01-20   1.0             Converted and improved from my GoLang implementation
*/

package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strconv"

	_ "github.com/denisenkom/go-mssqldb"
	//"errors"
)

var db *sql.DB

var server string  
var port = 1433
var user string
var password string
var database string

type constants struct {
	xImageSize        int
	yImageSize        int
	xStorageSize      int
	yStorageSize      int
	pixGrowthMax      int
	workingPath       string
	sourceCsvFileOrig string
	sourceCsvFile     string
	outputPng         string
	inFilename        string
	outPngFilename    string
	postcodeFilter    string
	sourceURL         string
}

func initConstants(c *constants) {
	c.xImageSize = 600
	c.yImageSize = 1000
	c.xStorageSize = 1000
	c.yStorageSize = 2000
	c.pixGrowthMax = 20
	c.workingPath = "n:/tmp/GBfull"
	c.sourceCsvFileOrig = "GB_full.txt" //filename in zip from intetnet
	c.sourceCsvFile = "GB_full.csv"     //file to process into map
	c.outputPng = "uk_sql.bmp"
	c.inFilename = c.workingPath + "/" + c.sourceCsvFile //"N:/code/GB_full.csv"
	c.outPngFilename = c.workingPath + "/" + c.outputPng //"N:/code/uk.png"
	c.postcodeFilter = ""                                //"^RG.*"
	c.sourceURL = "http://download.geonames.org/export/zip/GB_full.csv.zip"

}

type  pcLongLat struct {
	Postcode string
	Longitude float64
	Latitude float64
	LongitudeStr string
	LatitudeStr string
}


func readMap() {
	fmt.Println("Reading CSV...")

	ctx := context.Background()
	if db == nil {
		fmt.Println("ERROR: DB not connected...")
		return
	}

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		fmt.Println("ERROR: DB not alive...")
		return
	}

	csvfile, err := os.Open(c.inFilename)
	if err != nil {
		fmt.Println("ERROR: Couldnt open CSV...")
		return
	}

	// Parse the file
	r := csv.NewReader(csvfile)
	r.Comma = '\t'

	// Iterate through the records
	fmt.Print("500s of rows written to Azure SQL:")
	i := 0
	var records []pcLongLat
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		longitStr := record[10]
		latitStr := record[9]
		longit, err1 := strconv.ParseFloat(record[10], 64)
		latit, err2 := strconv.ParseFloat(record[9], 64)
		Postcode := record[1]

		inScope := true
		if len(c.postcodeFilter) > 0 {
			inScope, _ = regexp.MatchString(c.postcodeFilter, record[1])
		}

		if inScope {
			if err1 == nil && err2 == nil {

				var rec pcLongLat
				rec.Postcode = Postcode
				rec.Longitude = longit
				rec.Latitude = latit
				rec.LongitudeStr = longitStr
				rec.LatitudeStr = latitStr
				records = append(records, rec)

				if i % 500 == 0 {
					writeSqlBatch(records)
					// flush buffer
					records = nil
				}
			}
			i++
			if i%500 == 0 {
				fmt.Print(".")
			}
		}
	}
	// write final 0..99 records
	writeSqlBatch(records)
	fmt.Println()
}

func writeSqlBatch(records []pcLongLat) {
	// write a batch of records to DB.
	// Have to do this as values string since parameterised only supports single row at a time
	if len(records) > 0 {
		ctx := context.Background()
		valueStr := " VALUES "
		firstRec := true

		for _,r := range records {
			if !firstRec { valueStr += "," }
			valueStr += "('" + r.Postcode + "'," + r.LongitudeStr + "," + r.LatitudeStr + ")"
			firstRec = false
		}

		// insert 100 values
		insertStr := "INSERT INTO stage.rawPostcodes (Postcode, Longitude, Latitude) " + valueStr
		db.ExecContext(ctx, insertStr)
	}
}

func execSqlSproc_spRawToProcess() {
	// run a sproc, no params
	ctx := context.Background()
	execStr := "EXECUTE stage.spRawToProcess"
		db.ExecContext(ctx, execStr)
}

func execSqlSproc_spGrowProcessed() {
	// run a sproc, no params
	ctx := context.Background()
	execStr := "EXECUTE process.spGrowProcessed"
		db.ExecContext(ctx, execStr)
}

func execSqlSproc_spGenerateImage() {
	// run a sproc, no params
	ctx := context.Background()
	execStr := "EXECUTE img.spGenerateImage"
		db.ExecContext(ctx, execStr)
}


func writeImageFile(file string) {
	fmt.Println("Write image to file...")

	outFile, err := os.OpenFile(c.outPngFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		return
	}

	outFile.WriteString(file)

	outFile.Close()

}

func getImage() {
	fmt.Println("Retrieving image from DB...")

	ctx := context.Background()
	if db == nil {
		fmt.Println("ERROR: DB not connected...")
		return
	}

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		fmt.Println("ERROR: DB not alive...")
		return
	}

	tsql := "SELECT b  FROM img.images ORDER BY byteCount"

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return
	}

	defer rows.Close()

	outFile, err := os.OpenFile(c.outPngFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		return
	}

	var count int

	// Iterate through the result set.
	for rows.Next() {
		var btn []byte

		// Get values from row.
		err := rows.Scan(&btn)
		if err != nil {
			return
		}

		outFile.Write(btn)

		count++
	}
	fmt.Println()

	outFile.Close()

}

var c constants

func main() {
	//var mapVar mapStrct

	// Get Azure SQL info
	user = os.Getenv("AZURESQLUSER")
	server = os.Getenv("AZURESQLSVRURL") 
	password = os.Getenv("AZURESQLPWD")  
	database = os.Getenv("AZURESQLDB") 

	if user == "" || server == "" || password == "" || database == "" {
		fmt.Println("ERROR: Environment variables not set")
		return
	}

	// fixed seed so colours are predictable
	rand.Seed(0)
	initConstants(&c)

	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)

	var err error

	// Create connection pool
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Printf("Connected!\n")

	readMap()
	execSqlSproc_spRawToProcess()
	//execSqlSproc_spGrowProcessed()
	execSqlSproc_spGenerateImage()
	getImage()
}


