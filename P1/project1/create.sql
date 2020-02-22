DROP TABLE IF EXISTS Items;
DROP TABLE IF EXISTS Categories;
DROP TABLE IF EXISTS Bids;
DROP TABLE IF EXISTS Users;

CREATE TABLE Items (
   ItemID INTEGER,
   Name CHAR[10000],
   Currently FLOAT,
   First_Bid FLOAT,
   Buy_Price FLOAT,
   Number_of_Bids INTEGER,
   Started DATE,
   Ends DATE,
   SellerID CHAR(10000),
   Description CHAR(10000),
   PRIMARY KEY(ItemID),
   FOREIGN KEY(SellerID) REFERENCES Users(UserID)
);

CREATE TABLE Categories (
   ItemID INTEGER,
   Category CHAR(10000),
   PRIMARY KEY(ItemID, Category),
   FOREIGN KEY(ItemID) REFERENCES Items(ItemID)
);

CREATE TABLE Bids (
   ItemID INTEGER,
   UserID CHAR(10000),
   Time DATE,
   Amount FLOAT,
   PRIMARY KEY(ItemID, UserID, Amount),
   UNIQUE(ItemID, Time),
   FOREIGN KEY(ItemID) REFERENCES Items(ItemID)
   FOREIGN KEY(UserID) REFERENCES Users(UserID)
);

CREATE TABLE Users (
   UserID CHAR(100),
   Location CHAR(100),
   Country CHAR(100),
   Rating INTEGER,
   PRIMARY KEY(UserID)
);
