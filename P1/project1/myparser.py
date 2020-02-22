
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS145 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}


"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):

    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file

        fitems = open('items.dat', 'a')
        fcategories = open('categories.dat', 'a')
        fbids = open('bids.dat', 'a')
        fusers = open('users.dat', 'a')

        for item in items:
            # create a table for items
            ItemID = item['ItemID']
            Name = item['Name'].replace('\"', '\"\"')
            Currently = transformDollar(item['Currently'])

            try:
                Buy_Price = transformDollar(item['Buy_Price'])
            except KeyError:
                Buy_Price = "None"

            First_Bid = transformDollar(item['First_Bid'])
            Number_of_Bids = item['Number_of_Bids']
            Started = transformDttm(item['Started'])
            Ends = transformDttm(item['Ends'])
            seller = item['Seller']
            sellerID = seller['UserID']
            if item['Description'] == None:
                Description = "None"
            else:
                Description = str(item['Description'].replace('\"', '\"\"'))

            fitems.write(ItemID + "|\"" +  Name + "\"|" + Currently + "|" + First_Bid + "|" + Buy_Price + "|" + Number_of_Bids + "|\"" + Started + "\"|\"" + Ends + "\"|\"" + sellerID +"\"|\""+ Description + "\"\n")

            # create a table for catories
            for category in item['Category']:
                fcategories.write(ItemID + "|\""+ category + "\"\n")

            # create a table for bids
            bids = item['Bids']
            if bids != None:
                for element in bids:
                    bid = element['Bid']
                    bidder = bid['Bidder']
                    userID = bidder['UserID']
                    time = transformDttm(bid['Time'])
                    amount = transformDollar(bid['Amount'])
                    fbids.write(ItemID + "|\"" +  userID + "\"|\"" + time + "\"|" + amount + "\n")

            # create a table for user part: seller
            seller_Rating = seller['Rating']
        
            try:
                item_Location = item['Location'].replace('\"', '\"\"')
            except KeyError:
                item_Location = "None"

            try:
                item_Country = item['Country']
            except KeyError:
                item_Country = "None"

            fusers.write("\"" + sellerID + "\"|\""+ item_Location+"\"|\"" + item_Country + "\"|" +seller_Rating + "\n" )

            # part: bidder
            if bids != None :
                for element in bids:
                    bid = element['Bid']
                    bidder = bid['Bidder']
                    userID = bidder['UserID']
                    try:
                        Bidder_location = bidder['Location'].replace('\"', '\"\"')
                    except KeyError:
                        Bidder_location = "None"
                    try:
                        Bidder_country = bidder['Country']
                    except KeyError:
                        Bidder_country = "None"
                    Bidder_Rating = bidder['Rating']

                    fusers.write("\"" + userID + "\"|\"" + Bidder_location + "\"|\""+ Bidder_country + "\"|"+ Bidder_Rating+"\n")

    fitems.close()
    fcategories.close()
    fbids.close()
    fusers.close()





"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print "Success parsing " + f

if __name__ == '__main__':
    main(sys.argv)
