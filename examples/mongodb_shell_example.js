// MongoDB Shell Script Example
// Run this with: mongosh --file mongodb_shell_example.js

// Connect to a database (will create it if it doesn't exist)
db = db.getSiblingDB('bookstore');

// Create a collection (will create it if it doesn't exist)
// Clear existing data for demonstration purposes
db.books.drop();

// Insert documents into the collection
print("Inserting books into the collection...");
db.books.insertMany([
  {
    title: "The Hobbit",
    author: "J.R.R. Tolkien",
    year: 1937,
    pages: 310,
    genres: ["fantasy", "adventure"]
  },
  {
    title: "To Kill a Mockingbird",
    author: "Harper Lee",
    year: 1960,
    pages: 281,
    genres: ["fiction", "drama"]
  },
  {
    title: "1984",
    author: "George Orwell",
    year: 1949,
    pages: 328,
    genres: ["dystopian", "science fiction"]
  }
]);

// Find all documents in the collection
print("\nAll books in the collection:");
const allBooks = db.books.find().toArray();
printjson(allBooks);

// Find books with a specific condition
print("\nBooks published after 1950:");
const modernBooks = db.books.find({ year: { $gt: 1950 } }).toArray();
printjson(modernBooks);

// Update a document
print("\nUpdating '1984' page count...");
db.books.updateOne(
  { title: "1984" },
  { $set: { pages: 336 } }
);

// Find the updated document
print("\nVerifying the update:");
const updatedBook = db.books.findOne({ title: "1984" });
printjson(updatedBook);

// Aggregate example
print("\nAverage number of pages per book:");
const avgPagesResult = db.books.aggregate([
  { $group: { _id: null, avgPages: { $avg: "$pages" } } }
]).toArray();
printjson(avgPagesResult);

print("\nMongoDB Shell script completed!"); 