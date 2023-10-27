FILENAME: test8.db
1: CREATE TABLE student_123 (name TEXT, grade REAL, piazza INTEGER);
1: INSERT INTO student_123 VALUES ('James', 4.0, 1);
1: INSERT INTO student_123 VALUES ('Yaxin', 4.0, 2);
1: INSERT INTO student_123 VALUES ('Li', 3.2, 2);
1: SELECT * FROM student_123 ORDER BY piazza, grade;
FILENAME: test8a.db
2: CREATE TABLE student_123 (name TEXT, grade REAL, piazza INTEGER);
2: INSERT INTO student_123 VALUES ('James Jr.', 4.0, 1);
2: INSERT INTO student_123 VALUES ('Yaxin Jr.', 4.0, 2);
2: INSERT INTO student_123 VALUES ('Li Jr.', 3.2, 2);
2: SELECT * FROM student_123 ORDER BY piazza, grade;
FILENAME: test8b.db
3: CREATE TABLE student_123 (name TEXT, grade REAL, piazza INTEGER);
3: INSERT INTO student_123 VALUES ('James 3.', 4.0, 1);
3: INSERT INTO student_123 VALUES ('Yaxin 3.', 4.0, 2);
3: INSERT INTO student_123 VALUES ('Li 3.', 3.2, 2);
3: SELECT * FROM student_123 ORDER BY piazza, grade;
1: UPDATE student_123 SET piazza = 4;
2: UPDATE student_123 SET piazza = 5;
3: UPDATE student_123 SET piazza = 6;
1: SELECT * FROM student_123 ORDER BY piazza, grade;
2: SELECT * FROM student_123 ORDER BY piazza, grade;
3: SELECT * FROM student_123 ORDER BY piazza, grade;
3: CLOSE
1: CLOSE
2: CLOSE