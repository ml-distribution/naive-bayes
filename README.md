HW4a: Naive Bayes on Hadoop
===========================

This is a colleciton of Java files using the Hadoop framework to build a 
distributed version of the Naive Bayes classifier. It consists of four MR
tasks: two for training, one for joining the trained model to the test data,
and one for classification.

To create the .jar archive:

    javac *.java
    jar cfm NBTask.jar Manifest.txt *.class

*By specifying some `Manifest.txt` file, you can point Java--and hence, Hadoop--to
the class with the `main` method. Just include the following line (followed by
a newline) in the text file:*

    Main-Class: Package.ClassName

To run the job:

    hadoop jar NBTask.jar -D train=/path/to/training/data -D test=/path/to/test/data -D output=/output/dir [-D reducers=10]
    
