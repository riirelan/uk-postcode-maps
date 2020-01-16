/*
Created on Wed Jan 15 12:37:15 2020

@author: rich_ireland@hotmail.com
@version: 1.0
@versionDate: 2020-01-15

This python3 script
* Downloads all the UK postcode longitude,latitude from
      http://download.geonames.org/export/zip/GB_full.csv.zip
* Extracts the download
* Reads the resulting CSV of long/lats into a 2D array
* Optionally, 'grows' each point outwards to infil unallocated long/lats at
      a postcode district level
* Draws the resulting image with randomly assigned postcode district colouring
* Saves the image as PNG

History
Date         Version         Action
2020-01-15   1.0             Converted and improved from my GoLang implementation
*/

package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
)

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
	c.outputPng = "uk_go.png"
	c.inFilename = c.workingPath + "/" + c.sourceCsvFile //"N:/code/GB_full.csv"
	c.outPngFilename = c.workingPath + "/" + c.outputPng //"N:/code/uk.png"
	c.postcodeFilter = ""                                //"^RG.*"
	c.sourceURL = "http://download.geonames.org/export/zip/GB_full.csv.zip"

}

func getSourceCsv() error {
	fmt.Println("Retrieve CSV from URL and unzip...")

	resp, err := http.Get(c.sourceURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	f, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}
	io.Copy(f, resp.Body)
	tempName := f.Name()
	f.Close()

	r, err := zip.OpenReader(tempName)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, zipF := range r.File {
		// only handle expected file in archive...
		if zipF.Name != c.sourceCsvFileOrig {
			fmt.Println("   Skipped archive entry: ", zipF.Name)
			continue
		}

		// try to make working directory and ignore already exists error
		//  doesnt catch no permissions error?!
		_ = os.Mkdir(c.workingPath, os.ModeDir)

		fOutPath := c.workingPath + "/" + c.sourceCsvFileOrig
		// open output file
		outFile, err := os.OpenFile(fOutPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
		if err != nil {
			return err
		}
		// open input file from zip
		inFile, err := zipF.Open()
		if err != nil {
			return err
		}

		_, err = io.Copy(outFile, inFile)
		outFile.Close()
		inFile.Close()

		fOutPathRename := c.workingPath + "/" + c.sourceCsvFile
		os.Rename(fOutPath, fOutPathRename)
	}
	return nil
}

func findRange() (rangeEasting, rangeNorthing, minEasting, minNorthing int64) {
	fmt.Println("Finding CSV long/lat bounds to size storage...")

	csvfile, err := os.Open(c.inFilename)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// Parse the file
	r := csv.NewReader(csvfile)
	r.Comma = '\t'

	var minEastingF float64 = 59744300000 //-1
	var maxEastingF float64 = -1
	var minNorthingF float64 = 56697310000 //-1
	var maxNorthingF float64 = -1

	rangeEasting = 0
	rangeNorthing = 0

	// Iterate through the records
	i := 0
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		longit, err1 := strconv.ParseFloat(record[10], 64)
		latit, err2 := strconv.ParseFloat(record[9], 64)

		inScope := true
		if len(c.postcodeFilter) > 0 {
			inScope, _ = regexp.MatchString(c.postcodeFilter, record[1])
		}

		if inScope {
			if err1 == nil && err2 == nil {
				//easting, northing, zoneNum, zoneLetter, _ := UTM.FromLatLon(latit, longit, true)
				//			easting, northing, _, _, _ := UTM.FromLatLon(latit, longit, true)

				easting := (longit + 180) * 10000
				northing := (latit) * 10000

				if easting > maxEastingF || maxEastingF == -1 {
					maxEastingF = easting
				}
				if easting < minEastingF || minEastingF == -1 {
					minEastingF = easting
				}
				if northing > maxNorthingF || maxNorthingF == -1 {
					maxNorthingF = northing
				}
				if northing < minNorthingF || minNorthingF == -1 {
					minNorthingF = northing
				}

			}
			i++
		}
	}
	rangeEasting = int64(maxEastingF - minEastingF)
	rangeNorthing = int64(maxNorthingF - minNorthingF)
	minEasting = int64(minEastingF)
	minNorthing = int64(minNorthingF)

	return rangeEasting, rangeNorthing, minEasting, minNorthing
}

type mapStrct struct {
	xy                   [][]uint8
	PostcodeArea         [][]string
	img                  *image.NRGBA //*image.Gray
	xStorageRatio        float64
	yStorageRatio        float64
	xImageRatio          float64
	yImageRatio          float64
	xStorageToImageRatio float64
	yStorageToImageRatio float64
	rangeEasting         int64
	rangeNorthing        int64
	xOffset              int64
	yOffset              int64
	postcodeCount        int64
	uniqueStorageCount   int64
}

func initMap(m *mapStrct, rangeEasting, rangeNorthing, minEasting, minNorthing int64) {
	m.rangeEasting = rangeEasting
	m.rangeNorthing = rangeNorthing
	m.xOffset = minEasting
	m.yOffset = minNorthing

	m.xStorageRatio = float64(m.rangeEasting) / float64(c.xStorageSize)
	m.yStorageRatio = float64(m.rangeNorthing) / float64(c.yStorageSize)

	m.xImageRatio = float64(m.rangeEasting) / float64(c.xImageSize)
	m.yImageRatio = float64(m.rangeNorthing) / float64(c.yImageSize)

	m.xStorageToImageRatio = m.xImageRatio / m.xStorageRatio
	m.yStorageToImageRatio = m.yImageRatio / m.yStorageRatio

	m.xy = make([][]uint8, c.xStorageSize+1)
	m.PostcodeArea = make([][]string, c.xStorageSize+1)
	for i := 0; i < c.xStorageSize+1; i++ {
		m.xy[i] = make([]uint8, c.yStorageSize+1)
		m.PostcodeArea[i] = make([]string, c.yStorageSize+1)
	}

	//m.img = image.NewGray(image.Rect(0,0,xImageSize, yImageSize))
	m.img = image.NewNRGBA(image.Rect(0, 0, c.xImageSize, c.yImageSize))
}

func printMap(m *mapStrct) {
	fmt.Println("xStorageRatio             ", m.xStorageRatio)
	fmt.Println("yStorageRatio             ", m.yStorageRatio)
	fmt.Println("xImageRatio               ", m.xImageRatio)
	fmt.Println("yImageRatio               ", m.yImageRatio)
	fmt.Println("xStorageToImageRatio      ", m.xStorageToImageRatio)
	fmt.Println("yStorageToImageRatio      ", m.yStorageToImageRatio)
	fmt.Println("rangeEasting              ", m.rangeEasting)
	fmt.Println("rangeNorthing             ", m.rangeNorthing)
	fmt.Println("xOffset                   ", m.xOffset)
	fmt.Println("yOffset                   ", m.yOffset)
	fmt.Println("postcodeCount             ", m.postcodeCount)
	fmt.Println("uniqueStorageCount        ", m.uniqueStorageCount)
	fmt.Println()
}

func readMap(m *mapStrct) {
	fmt.Println("Reading CSV...")
	csvfile, err := os.Open(c.inFilename)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// Parse the file
	r := csv.NewReader(csvfile)
	r.Comma = '\t'

	// Iterate through the records
	i := 0
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		longit, err1 := strconv.ParseFloat(record[10], 64)
		latit, err2 := strconv.ParseFloat(record[9], 64)
		Postcode := record[1]

		PostcodeSplit := strings.Fields(Postcode)
		PostcodeDistrict := PostcodeSplit[0]

		inScope := true
		if len(c.postcodeFilter) > 0 {
			inScope, _ = regexp.MatchString(c.postcodeFilter, record[1])
		}

		if inScope {
			if err1 == nil && err2 == nil {

				easting := (longit + 180) * 10000
				northing := (latit) * 10000

				if m.xy[int64((easting-float64(m.xOffset))/m.xStorageRatio)][int64((northing-float64(m.yOffset))/m.yStorageRatio)] == 0 {
					m.uniqueStorageCount += 1
					m.PostcodeArea[int64((easting-float64(m.xOffset))/m.xStorageRatio)][int64((northing-float64(m.yOffset))/m.yStorageRatio)] = PostcodeDistrict //[0:2]
				}
				m.xy[int64((easting-float64(m.xOffset))/m.xStorageRatio)][int64((northing-float64(m.yOffset))/m.yStorageRatio)] += 1
				m.postcodeCount += 1

				i++
			}
		}
	}
}

func growBoundsOnce2(m *mapStrct, distance int) (infils int) {
	/* perform one pass on the map struct, expanding into empty
	   space. It does this by looking left and up from each point to
	   find the next populated point within [distance] points, and
	   only if it finds one then,
	      if that point is the same postcode, all point between are
		     claimed as that postcode as well
	      if that point is a different postcode, half the points between
	*/
	infils = 0

	for x := 1; x < c.xStorageSize-1; x++ {
		for y := 1; y < c.yStorageSize-1; y++ {
			pp := m.PostcodeArea[x][y]

			// look left and up, max n squares
			n := distance
			xx := x
			fullFill := false
			halfFill := false
			hasGap := false

			if pp != "" {
				for xx = x - 1; xx > x-n && xx > 0; xx-- {
					if m.PostcodeArea[xx][y] == pp {
						fullFill = true
						break
					}
					if m.PostcodeArea[xx][y] == "" {
						hasGap = true
					}
					if m.PostcodeArea[xx][y] != pp && m.PostcodeArea[xx][y] != "" {
						halfFill = true
						break
					}
				}

				xTgt := x

				if halfFill {
					xTgt = x - ((x - xx) / 2)
				}
				if fullFill {
					xTgt = xx
				}

				if hasGap && (halfFill || fullFill) {
					if xTgt != x {
						for xx := xTgt; xx < x; xx++ {
							m.PostcodeArea[xx][y] = pp
							m.xy[xx][y] = 1
							infils++
						}
						//fmt.Println(".")
					}
					//fmt.Print(":")
				}
			}

			yy := y
			fullFill = false
			halfFill = false
			hasGap = false

			if pp != "" {
				for yy = y - 1; yy > y-n && yy > 0; yy-- {
					if m.PostcodeArea[x][yy] == pp {
						fullFill = true
						break
					}
					if m.PostcodeArea[x][yy] == "" {
						hasGap = true
					}
					if m.PostcodeArea[x][yy] != pp && m.PostcodeArea[x][yy] != "" {
						halfFill = true
						break
					}
				}

				yTgt := y

				if halfFill {
					yTgt = y - ((y - yy) / 2)
				}
				if fullFill {
					yTgt = yy
				}

				if hasGap && (halfFill || fullFill) {
					if yTgt != y {
						for yy := yTgt; yy < y; yy++ {
							m.PostcodeArea[x][yy] = pp
							m.xy[x][yy] = 1
							infils++
						}
						//fmt.Println(".")
					}
					//fmt.Print(":")
				}
			}
		}
	}
	return infils
}

type rgb struct {
	r uint8
	g uint8
	b uint8
}

func getColour2(pcArea string, m *map[string]rgb) (c color.NRGBA) {
	lookup, ok := (*m)[pcArea]
	if ok == false {

		var rgbV rgb
		rgbV.r = uint8(rand.Intn(255))
		rgbV.g = uint8(rand.Intn(255))
		rgbV.b = uint8(rand.Intn(255))

		(*m)[pcArea] = rgbV
		lookup, ok = (*m)[pcArea]
	}

	return color.NRGBA{lookup.r, lookup.g, lookup.b, 255}
}

func generateImage(m *mapStrct) {
	// convert the final filled point map to image
	fmt.Println("Convert final structure to image...")
	var col uint8

	mp := make(map[string]rgb)

	for x := 0; x < c.xStorageSize; x++ {
		for y := 0; y < c.yStorageSize; y++ {
			col = m.xy[x][y]
			xImg := int(float64(x) / m.xStorageToImageRatio)
			yImg := c.yImageSize - int(float64(y)/m.yStorageToImageRatio)

			if col > 0 {
				m.img.SetNRGBA(xImg, yImg, getColour2(m.PostcodeArea[x][y], &mp))
			} else {
				m.img.SetNRGBA(xImg, yImg, color.NRGBA{255, 255, 255, 255})
			}
		}
	}

	var imgBuf bytes.Buffer
	//err := jpeg.Encode(&imgBuf, mapVar.img, nil)
	err := png.Encode(&imgBuf, m.img)

	if err != nil {
		panic(err)
	}
	fo, err := os.Create(c.outPngFilename)
	if err != nil {
		panic(err)
	}
	fw := bufio.NewWriter(fo)
	fw.Write(imgBuf.Bytes())

}

func grow(m *mapStrct) {
	// Expand the points to fill nearby empty space
	k := 3
	maxK := k
	reduceK := 5
	fmt.Println("Expanding points to fill. (this may take a few minutes)...")
	for {
		infils := growBoundsOnce2(m, k)
		if infils > 0 {
			fmt.Println("k: ", k, "/", maxK, ":", c.pixGrowthMax, " Infilled bits: ", infils)
		}

		if infils == 0 && k < c.pixGrowthMax {
			k++
		}
		if infils == 0 && k >= c.pixGrowthMax {
			break
		}
		//if infils > 0  && k <  pixGrowthMax { k = 3 }
		if infils > 50 && k < c.pixGrowthMax {
			if maxK > reduceK+3 {
				k = maxK - reduceK
			} else {
				k = 3
			}
		}
		if infils > 0 && infils <= 20 && k < c.pixGrowthMax {
			k++
		}
		if k > maxK {
			maxK = k
		}
	}
}

var c constants

func main() {
	var mapVar mapStrct

	// fixed seed so colours are predictable
	rand.Seed(0)
	initConstants(&c)

	if getSourceCsv() != nil {
		print("ERROR: Failed to retrieve postcode CSV from URL and unzip")
		return
	}

	rangeEasting, rangeNorthing, minEasting, minNorthing := findRange()
	initMap(&mapVar, rangeEasting, rangeNorthing, minEasting, minNorthing)
	printMap(&mapVar)
	readMap(&mapVar)
	printMap(&mapVar)
	grow(&mapVar)
	generateImage(&mapVar)
}
