# Usage:
#   (run all test files)
#   > python tester.py
#   
#   (run join tests; searches for test.join.[0-9][0-9].sql)
#   > python tester.py join
# 
# Sources:
# https://stackoverflow.com/questions/977491/comparing-two-txt-files-using-difflib-in-python
# https://docs.python.org/3/library/os.html#os.popen
# https://docs.python.org/3/library/re.html
# Inspiration from Brendan and Matthew 
# 
# Work provided on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE. You are solely responsible for determining the appropriateness of using or redistributing the Work and assume any risks associated with Your use or redistribution.
# 
# Version: 0.0.2
# 
# Good luck ~ Han
###################################

# standard library
import os
import difflib
import re
import sys
from collections import defaultdict

# get type of test to run
query_type = run_all_tests =  '[a-z_\\-]+'
if len(sys.argv) > 1:
    if '--help' in sys.argv or '-h' in sys.argv:
        print('''usage:
    # run all tests
    python tester.py

    # run join tests
    python tester.py join

    # run where tests
    python tester.py where''')
        exit()
    query_type = sys.argv[1]


test_files = passed_tests = failed_tests = 0
failed_test_types = defaultdict(list)
test_types = defaultdict(int)
# test each file
for each_file in os.listdir():
    if re.match(f'test\\.{query_type}\\.[\\d]{{2}}.sql', each_file):
        test_files += 1
        test_type = re.search(r'(?<=test\.)[a-z_\-]+', each_file).group(0)
        test_types[test_type] += 1
        your_output = os.popen(f'python cli.py {each_file}').read()
        sqlite_output = os.popen(f'python cli.py {each_file} --sqlite').read()
        if your_output != sqlite_output:
            print(f'===>Fail: {each_file}<===')
            failed_tests += 1
            failed_test_types[test_type].append(each_file)
            for x in difflib.ndiff(your_output.split('\n'), sqlite_output.split('\n')):
                print(x)
        else:
            print(f'===Pass: {each_file}===')
            passed_tests += 1

# show run stats
print('-' * 70)
if failed_tests:
    print(f'Failed {failed_tests} tests')
    for fail_type, tests in failed_test_types.items():
        print(f' {fail_type.upper()} (failures={len(tests)}) out of {test_types[fail_type]}: ', end='')
        print(*tests)
    print()
print(f'Passed ({passed_tests}/{test_files}) {(passed_tests/test_files if test_files else 1) * 100:.2f}% of {"ALL" if query_type == run_all_tests else query_type.upper()} tests')



# import os

# files = os.listdir()
# for file in files:
#     if file.startswith("test.") and file != "test.py":
#         test_path = f'./output_{file[:len(file)-4]}.txt'
#         truth_path = f'./output_{file[:len(file)-4]}_sql.txt'
#         if os.path.isfile(test_path):
#             os.remove(test_path)
#             os.remove(truth_path)

#         os.system(f"python3 cli.py {file} >> {test_path}")
#         os.system(f'python3 cli.py {file} --sqlite >> {truth_path}')
#         ans = os.system(f'diff {test_path} {truth_path}')
#         if ans != 0:
#             os.system(f'echo {file} failed')

#         os.remove(test_path)
#         os.remove(truth_path)