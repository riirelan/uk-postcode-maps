# -*- coding: utf-8 -*-
"""
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

"""

import csv
from PIL import Image
import random
from urllib.request import urlopen
from zipfile import ZipFile
import tempfile
import os

class MapException(Exception):
    pass

class constants:
    sourceURL         = 'http://download.geonames.org/export/zip/GB_full.csv.zip'
    workingPath       = 'n:/tmp/GBfull'
    sourceCsvFileOrig = 'GB_full.txt'
    sourceCsvFile     = 'GB_full.csv'
    postcodeFilter    = "^RG.*"
    xImageSize       = 600
    yImageSize       = 1000
    xStorageSize     = 1000
    yStorageSize     = 2000
    pixGrowthMax     = 20
    
    def setStatics():
        constants.inFilename = constants.workingPath + "/" + constants.sourceCsvFile
        constants.outPngFilename = constants.workingPath + "/" + "uk.png"      
        

def getSourceCsv():
    """ Read the ZIP file from the Internet to a temp file, unzip the file
        and rename the txt file to csv for clarity."""
    try:
        with urlopen(constants.sourceURL) as response:
            tempZip = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
            tempFileName = tempZip.name
    
            tempZip.write(response.read())
            tempZip.close()
            
    except:
        raise MapException("Failed to read file from URL")
    
    try:
        zf = ZipFile(tempFileName)#"n:/tmp/tzip.zip")
        zf.extractall(path = constants.workingPath) # GB_full.txt results
        zf.close()
        
    except:
        raise MapException("Failed to unzip downloaded file")

    try:
        os.rename(constants.workingPath + '/' + constants.sourceCsvFileOrig, 
                  constants.workingPath + '/' + constants.sourceCsvFile)
        
    except:
        raise MapException("Zip does not contain GB_full.txt or GB_full.csv already exists")


class point:
    """ Class to record a compressed postcode point
        (compressed from easting/northing to storage x/y density)"""
    def __init__(self, x, y):
        self.count = 0
        self.PostCode = ""
        self.x = x
        self.y = y
        
    def setPostcode(self, postcode):
        self.PostCode = postcode
        
    def getPostcode(self) -> str:
        return self.PostCode 
        
    def increment(self):
        self.count += 1
        
    def setCount(self, count):
        self.count = count        
        
    def getCount(self):
        return self.count
        
    def print(self):
        print("point: ", self.x, "  ", self.y, "   ", self.PostCode, " ", self.count)

class colourMap:
    """ Class to record postcode colours for image  """
    def __init__(self, r,g,b):
        self.r = r
        self.g = g
        self.b = b
        
    def getRed(self):
        return self.r
    
    def getGreen(self):
        return self.g
    
    def getBlue(self):
        return self.b

class mapStrct:
    """ Class to record postcodes and manipulate them. Records
        * Compressed CSV data (with multiple easting/northings in a single point)
        * Map Image
        And manipulate 
        * Find coundary range in CSv
        * Read CSV into compressed representation
        * Grow the compressed representation to fill nearby space
        * Convert compressed representation into image        
    """
    
    def __init__(self):
        """ Read the long/lat boundaries in the CSV and then init
            instance variables based on those values """
        self.findRange()
        self.xStorageRatio    = float(self.rangeEasting) / float(constants.xStorageSize)
        self.yStorageRatio    = float(self.rangeNorthing) / float(constants.yStorageSize)
        self.xImageRatio      = float(self.rangeEasting) / float(constants.xImageSize)
        self.yImageRatio      = float(self.rangeNorthing) / float(constants.yImageSize)
        self.xStorageToImageRatio = self.xImageRatio / self.xStorageRatio
        self.yStorageToImageRatio = self.yImageRatio / self.yStorageRatio
        self.uniqueStorageCount = 0
        self.postcodeCount = 0
        self.postcodeColourDict = {}
        self.xy = [[point(i,j) for j in range(constants.yStorageSize+1)] 
                for i in range(constants.xStorageSize+1)]     
        
    def incrementEastingNorthing(self, easting, northing, postcode):
        """ Init or add one to the postcode at the point """
        try:
            x = int((easting-float(self.xOffset))/self.xStorageRatio)
            y = int((northing-float(self.yOffset))/self.yStorageRatio)
            if self.xy[x][y].getCount() == 0:
                self.uniqueStorageCount += 1
                self.xy[x][y].setPostcode(postcode)
            self.xy[x][y].increment()
            self.postcodeCount += 1
        except IndexError:
            print("Error: e/n: ", easting, northing, "  x/y ", x,y)
        
    def print(self):
        """ print entire map struct """
        for x in range(self.xr):
            for y in range(self.yr):
                print("mapStrct at: ",x,y)
                self.xy[x][y].print()
                
    def getPostcodeColour(self, postcode):
        """ Find colour for postcode, or create new one if not found in dict """
        if postcode not in self.postcodeColourDict:
            rgbCol = colourMap(random.randint(0,255),random.randint(0,255),random.randint(0,255))
            self.postcodeColourDict[postcode] = rgbCol
        return self.postcodeColourDict[postcode]

    def generateImage(self):
        """ convert the final filled point map to image """
        print("Convert final structure to image...")
        
        self.img = Image.new('RGB', (constants.xImageSize+1, constants.yImageSize+1))
        print("   Image size: x/y ", constants.xImageSize, constants.yImageSize)
        xMax = int(float(constants.xStorageSize) / self.xStorageToImageRatio)
        yMax = int(float(constants.yStorageSize)/self.yStorageToImageRatio)

        # Loop over map struct writing pixels
        #   this does mean pixels get rewritten given resizing which isnt 
        #   optimal
        for x in range(constants.xStorageSize):
            for y in range(constants.yStorageSize):
                c = self.xy[x][y].getCount()
                
                rgbCol = self.getPostcodeColour(self.xy[x][y].getPostcode())
                
                xImg = int(float(x) / self.xStorageToImageRatio)
                yImg = constants.yImageSize - int(float(y)/self.yStorageToImageRatio)
                if c > 0:
                    self.img.putpixel((xImg,yImg),(rgbCol.getRed(),rgbCol.getGreen(),rgbCol.getBlue()))
                else:
                    self.img.putpixel((xImg,yImg),(255,255,255))
        self.img.show()

    def growBoundsOnce2(self, distance) -> int:
        """ perform one pass on the map struct, expanding into empty
            space. It does this by looking left and up from each point to 
            find the next populated point within [distance] points, and 
            only if it finds one then, 
               if that point is the same postcode, all point between are 
                   claimed as that postcode as well
               if that point is a different postcode, half the points between
        """
        infils = 0
        
       	for x in range(1,constants.xStorageSize-1):
            for y in range(1,constants.yStorageSize-1):
                pp = self.xy[x][y].getPostcode()

                # look left and up, max n squares
                n = distance
                xx = x
                fullFill = False
                halfFill = False
                hasGap   = False

                if pp != "":
                    for xx in range(x-1, x-n, -1):
                        if (xx <=0 ): break
#                    for (xx = x - 1; xx > x-n and xx > 0; xx--):
                        if self.xy[xx][y].getPostcode() == pp:
                            fullFill = True
                            break
                        elif self.xy[xx][y].getPostcode() == "":
                            hasGap = True
                        else:
                            halfFill = True
                            break

                    xTgt = x

                    if halfFill:
                        xTgt = int(x - ((x - xx) / 2))
                    if fullFill:
                        xTgt = xx

                    if hasGap and (halfFill or fullFill):
                        if xTgt != x:
                            for xx in range(xTgt, x):
                                self.xy[xx][y].setPostcode(pp)
                                self.xy[xx][y].setCount(1)
                                infils += 1

                yy = y
                fullFill = False
                halfFill = False
                hasGap   = False

                if pp != "":
                    for yy in range(y-1, y-n, -1):
                        if (yy <= 0): break
#                    for yy = y - 1; yy > y-n and yy > 0; yy--:
                        if self.xy[x][yy].getPostcode() == pp:
                            fullFill = True
                            break
                        elif self.xy[x][yy].getPostcode() == "":
                            hasGap = True
                        else:
                            halfFill = True
                            break

                    yTgt = y

                    if halfFill:
                        yTgt = int(y - ((y - yy) / 2))
                    if fullFill:
                        yTgt = yy

                    if hasGap and (halfFill or fullFill):
                        if yTgt != y:
                            for yy in range(yTgt, y):
#                            for yy = yTgt; yy < y; yy++:
                                self.xy[x][yy].setPostcode(pp)
                                self.xy[x][yy].setCount(1)
                                infils += 1

        return infils

    def findRange(self):
        """  Find the range of longitudes and latitudes in the input file """
        print("Finding CSV long/lat bounds to size storage...")
        
        minEastingF  = 59744300000.0
        maxEastingF  = -1.0
        minNorthingF = 56697310000.0
        maxNorthingF = -1.0
        self.rangeEasting = 0
        self.rangeNorthing = 0
        
        with open(constants.inFilename) as csvFile:
            r = csv.reader(csvFile, delimiter='\t')
            for row in r:
                try:
                    longit = float(row[10])
                    latit  = float(row[9])
                    
                except ValueError:
                    # Ignore invalid rows
                    pass
                
                else:
                
                    # check postcode filter TODO
                
                    easting  = (longit + 180) * 10000
                    northing = (latit) * 10000
                
                    if easting > maxEastingF or maxEastingF == -1:
                        maxEastingF = easting
                    if easting < minEastingF or minEastingF == -1:
                        minEastingF = easting
                    if northing > maxNorthingF or maxNorthingF == -1:
                        maxNorthingF = northing
                    if northing < minNorthingF or minNorthingF == -1:
                        minNorthingF = northing
    
        self.rangeEasting  = int(maxEastingF - minEastingF)
        self.rangeNorthing = int(maxNorthingF - minNorthingF)
        self.xOffset       = int(minEastingF)
        self.yOffset       = int(minNorthingF)
        
        print("   Easting range:   ", self.rangeEasting)
        print("   Northing range:  ", self.rangeNorthing)
        print("   Easting offset:  ", self.xOffset)
        print("   Northing offset: ", self.yOffset)
        
     #   return mapStrct(rangeEasting, rangeNorthing, minEasting, minNorthing)


    def readMap(self):
        """  Read the input file into object """
        print("Reading CSV...")
        
        with open(constants.inFilename) as csvFile:
            r = csv.reader(csvFile, delimiter='\t')
            for row in r:
                try:
                    longit = float(row[10])
                    latit  = float(row[9])
                    Postcode = row[1]
                    PostcodeDistrict = Postcode.split()[0]
                    
                except ValueError:
                    # Ignore invalid rows
                    pass
                
                else:
                    easting  = (longit + 180) * 10000
                    northing = (latit) * 10000
                    
                    self.incrementEastingNorthing(easting, northing, PostcodeDistrict)
                
    def grow(self):
        """ Expand the points to fill nearby empty space  """
        print("Expanding points to fill. (this may take a few minutes)...")
        k = 3
        maxK = k
        reduceK = 5
        while True:
            infils = self.growBoundsOnce2(k)
            if infils > 0:
                print("k: ", k, "/", maxK, ":", constants.pixGrowthMax, " Infilled bits: ", infils)
    
            if infils == 0 and k < constants.pixGrowthMax:
                k += 1
            if infils == 0 and k >= constants.pixGrowthMax:
                break
            if infils > 50 and k < constants.pixGrowthMax:
                if maxK > reduceK+3:
                    k = maxK - reduceK
                else:
                    k = 3
            if infils > 0 and infils <= 20 and k < constants.pixGrowthMax:
                k += 1
            if k > maxK:
                maxK = k
            
            
def main():
    constants.setStatics()
    f = mapStrct()
    f.readMap()
    f.grow()
    f.generateImage()