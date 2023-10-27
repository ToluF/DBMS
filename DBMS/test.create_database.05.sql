FILENAME: test5.db
1: CREATE TABLE student (name TEXT, grade REAL, piazza INTEGER);
1: INSERT INTO student VALUES ('James', 4.0, 1);
1: INSERT INTO student VALUES ('Yaxin', 4.0, 2);
1: INSERT INTO student VALUES ('Li', 3.2, 2);
1: UPDATE student SET grade = 3.0, piazza = 3 WHERE name = 'Yaxin';
1: SELECT * FROM student ORDER BY name;
1: CLOSE