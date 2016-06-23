head -500 vendor_full.csv > vendor500.txt
head -1000 vendor_full.csv > vendor1000.txt
head -1500 vendor_full.csv > vendor1500.txt
head -2000 vendor_full.csv > vendor2000.txt
head -2500 vendor_full.csv > vendor2500.txt
head -3000 vendor_full.csv > vendor3000.txt


python columns_to_rows.py ./config vendor500.txt trans_vendor500
python columns_to_rows.py ./config vendor1000.txt trans_vendor1000
python columns_to_rows.py ./config vendor1500.txt trans_vendor1500
python columns_to_rows.py ./config vendor2000.txt trans_vendor2000
python columns_to_rows.py ./config vendor2500.txt trans_vendor2500
python columns_to_rows.py ./config vendor3000.txt trans_vendor3000


echo "starting lipsum"
cat lipsum.txt | python attr_cleanup_mapper.py
echo "starting trans_vendor500"
cat trans_vendor500 | python attr_cleanup_mapper.py
echo "starting trans_vendor1000"
cat trans_vendor1000 | python attr_cleanup_mapper.py
echo "starting trans_vendor1500"
cat trans_vendor1500 | python attr_cleanup_mapper.py
echo "starting trans_vendor2000"
cat trans_vendor2000 | python attr_cleanup_mapper.py
echo "starting trans_vendor2500"
cat trans_vendor2500 | python attr_cleanup_mapper.py
echo "starting trans_vendor3000"
cat trans_vendor3000 | python attr_cleanup_mapper.py
