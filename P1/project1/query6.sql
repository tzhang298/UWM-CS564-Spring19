SELECT COUNT(DISTINCT Users.UserID)
FROM Users, Items, Bids
WHERE
  Users.UserID = Items.SellerID
AND
  Users.UserID = Bids.UserID;
