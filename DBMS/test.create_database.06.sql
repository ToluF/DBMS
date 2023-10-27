FILENAME: test6.db
1: CREATE TABLE student (name TEXT, grade REAL, piazza INTEGER);
1: INSERT INTO student VALUES ('James', 4.0, 1);
1: INSERT INTO student VALUES ('Yaxin', NULL, 2);
1: INSERT INTO student VALUES ('Li', 3.2, 2);
1: SELECT * FROM student WHERE student.grade > 3.5 ORDER BY student.piazza, grade;
1: SELECT * FROM student WHERE student.grade < 3.5 ORDER BY student.piazza, grade;
1: CLOSE