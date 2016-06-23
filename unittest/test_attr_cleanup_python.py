import imp
import unittest
import sys
sys.path.append('/Users/lcle/git/attr-cleanup')
from attr_cleanup_mapper import * 
from attr_cleanup_reducer import * 

class MapperTests(unittest.TestCase):

    def test_get_frequent_value(self):
        list1=["abc", "abc", "abc", "ab", "ab", "ab", "ab","abc","abc"]
        self.assertEqual(get_frequent_value(list1), "abc")
    
    def test_create_similarvalue_list(self):
        words=["abc", "abc", "abc", "ab", "ab", "ab", "ab"]
        similarWord="abc"
        list1=create_similarvalue_list(words, similarWord)
        self.assertEqual(create_similarvalue_list(words, similarWord), ["abc", "abc"])
    
    def test_isValueCompared(self):
        word="abc"
        comparedValues=["abc", "ab"]
        self.assertEqual(isValueCompared(word, comparedValues), True)

class ReducerTests(unittest.TestCase):
    def test_isTenPercentMissing(self):
        nbrOfBlanks = 4
        totalcount = 10
        self.assertEqual(isTenPercentMissing(nbrOfBlanks, totalcount), 1)

    def test_getSkewedCountPercentage(self):
        valueCount = {'': 2, 'strawberry': 5, 'strawberries': 3, 'Apple': 2, 'Applee': 1} 
        totalcount = 13
        assertEqualValue = "0~,2,0.15|strawberry,5,0.38|strawberries,3,0.23|Apple,2,0.15|Applee,1,0.08|"
        self.assertEqual(getSkewedCountPercentage(valueCount, totalcount), assertEqualValue)
        
def main():
    unittest.main()

#if __name__ == '__main__':
#    main()

#Mapper test cases
print "\n--------------EXECUTING MAPPER TEST CASES-------------------"
suite = unittest.TestLoader().loadTestsFromTestCase(MapperTests)
unittest.TextTestRunner(verbosity=2).run(suite)

#Reducer test cases
print "\n--------------EXECUTING REDUCER TEST CASES-------------------"
suite = unittest.TestLoader().loadTestsFromTestCase(ReducerTests)
#unittest.TextTestRunner(verbosity=2).run(suite)
