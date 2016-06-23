head -500 vendor_full.csv > vendor500.txt
head -1000 vendor_full.csv > vendor1000.txt
head -1500 vendor_full.csv > vendor1500.txt
head -2000 vendor_full.csv > vendor2000.txt
head -2500 vendor_full.csv > vendor2500.txt
head -3000 vendor_full.csv > vendor3000.txt


python columns_to_rows.py ./config vendor500.txt trans_cof1000
python columns_to_rows.py ./config vendor1000.txt trans_cof1000
python columns_to_rows.py ./config vendor1500.txt trans_cof1500
python columns_to_rows.py ./config vendor2000.txt trans_cof2000
python columns_to_rows.py ./config vendor2500.txt trans_cof2500
python columns_to_rows.py ./config vendor3000.txt trans_cof3000
