use catalog

db.createCollection("electronics")

//mongoimport --db=catalog --collection=electronics --file=/home/salah/Downloads/catalog.json

db.electronics.createIndex({type:1})

db.electronics.countDocuments({"type":"laptop"})

db.electronics.countDocuments({"type":"smart phone","screen size":6})

db.electronics.aggregate([{$group:{_id:null,total:{$avg:"screen size"}}}])

db.electronics.aggregate([{$match:{ type : "smart phone"}},
{$group: {_id:"{$type}", avg_val:{$avg:"$screen size"}}}])

//mongoexport --db catalog --collection electronics --fields _id,type,model --type csv --out electronics.csv

