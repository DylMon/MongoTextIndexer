#-------------------------------------------------------------------------
# AUTHOR: Dylan Monge
# FILENAME: db_connection_mongo.py
# SPECIFICATION: This program provides the functionality to manage documents
# within a MongoDB database and generate an inverted index for text retrieval.
# It includes functions to connect to the MongoDB database, create documents
# with term frequency data, update and delete documents, and output an
# inverted index.
# FOR: CS 4250- Assignment #2
# TIME SPENT: 3hrs
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient

def connectDataBase():

    # Create a database connection object using pymongo
    client = MongoClient('localhost', 27017)
    return client['document_db']

def remove_punctuation(text):
    # Define the set of punctuation characters
    punctuation = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''

    # Remove punctuation from the text
    # Replace each punctuation character with a space
    for char in punctuation:
        text = text.replace(char, ' ')

    return text

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # Remove punctuation before processing
    docText = remove_punctuation(docText.lower())

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    terms = docText.split()
    term_counts = {}
    for term in terms:
        if term in term_counts:
            term_counts[term] += 1
        else:
            term_counts[term] = 1

    # create a list of dictionaries to include term objects. [{"term", count, num_char}]
    term_objects = [{"term": term, "count": count, "num_char": len(term)} for term, count in term_counts.items()]

    #Producing a final document as a dictionary including all the required document fields
    document = {
        "docId": docId,
        "docText": docText,
        "docTitle": docTitle,
        "docDate": docDate,
        "docCat": docCat,
        "terms": term_objects
    }

    # Insert the document
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"docId": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    index = {}
    documents = col.find()
    for doc in documents:
        docTitle = doc["docTitle"]
        docId = doc["docId"]
        for term_obj in doc["terms"]:
            term = term_obj["term"]
            entry = f"{docTitle}:{docId}"
            if term in index:
                index[term] += f", {entry}"
            else:
                index[term] = entry
    return index