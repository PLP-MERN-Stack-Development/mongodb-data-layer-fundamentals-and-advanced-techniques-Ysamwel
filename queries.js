const { MongoClient } = require("mongodb");
require("dotenv").config();


const client = new MongoClient(process.env.MONGO_URI);

async function runQueries() {
  try {
    await client.connect();
    console.log(" Connected to MongoDB");

    const db = client.db("plp_bookstore");
    const books = db.collection("books");

    // --- BASIC QUERIES ---

    // 1. Find all books in a specific genre (e.g., Fiction)
    const fictionBooks = await books.find({ genre: "Fiction" }).toArray();
    console.log("Fiction Books:", fictionBooks);

    // 2. Find books published after 2010
    const recentBooks = await books.find({ published_year: { $gt: 2010 } }).toArray();
    console.log("Books published after 2010:", recentBooks);

    // 3. Find books by a specific author (e.g., "George Orwell")
    const authorBooks = await books.find({ author: "George Orwell" }).toArray();
    console.log("Books by George Orwell:", authorBooks);

    // 4. Update the price of a specific book
    const updateResult = await books.updateOne(
      { title: "1984" },
      { $set: { price: 15.99 } }
    );
    console.log(" Updated price result:", updateResult);

    // 5. Delete a book by its title
    const deleteResult = await books.deleteOne({ title: "The Catcher in the Rye" });
    console.log("Deleted book result:", deleteResult);

    // --- ADVANCED QUERIES ---

    // 6. Find books that are in stock and published after 2010
    const inStockRecent = await books
      .find({ in_stock: true, published_year: { $gt: 2010 } })
      .project({ title: 1, author: 1, price: 1, _id: 0 })
      .toArray();
    console.log(" In-stock books published after 2010:", inStockRecent);

    // 7. Sort books by price ascending
    const sortedAsc = await books.find().sort({ price: 1 }).toArray();
    console.log("Books sorted by price (ascending):", sortedAsc);

    // 8. Sort books by price descending
    const sortedDesc = await books.find().sort({ price: -1 }).toArray();
    console.log(" Books sorted by price (descending):", sortedDesc);

    // 9. Pagination (5 books per page)
    const page1 = await books.find().limit(5).toArray();
    const page2 = await books.find().skip(5).limit(5).toArray();
    console.log(" Page 1:", page1);
    console.log("Page 2:", page2);

    // --- AGGREGATION PIPELINES ---

    // 10. Average price of books by genre
    const avgPriceByGenre = await books.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
    ]).toArray();
    console.log("Average price by genre:", avgPriceByGenre);

    // 11. Author with the most books
    const topAuthor = await books.aggregate([
      { $group: { _id: "$author", totalBooks: { $sum: 1 } } },
      { $sort: { totalBooks: -1 } },
      { $limit: 1 },
    ]).toArray();
    console.log(" Author with most books:", topAuthor);

    // 12. Group books by publication decade
    const booksByDecade = await books.aggregate([
      {
        $group: {
          _id: { $subtract: [{ $divide: ["$published_year", 10] }, { $mod: [{ $divide: ["$published_year", 10] }, 1] }] },
          totalBooks: { $sum: 1 },
        },
      },
      { $project: { decade: { $multiply: ["$_id", 10] }, totalBooks: 1, _id: 0 } },
    ]).toArray();
    console.log("Books grouped by decade:", booksByDecade);

    // --- INDEXING ---

    // 13. Create index on title
    await books.createIndex({ title: 1 });
    console.log(" Index created on title");

    // 14. Create compound index on author and published_year
    await books.createIndex({ author: 1, published_year: 1 });
    console.log("Compound index created on author and published_year");

    // 15. Explain query performance
    const explainResult = await books.find({ title: "1984" }).explain("executionStats");
    console.log("\nExplain plan for indexed search:", explainResult.executionStats);

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
    console.log(" Connection closed.");
  }
}

runQueries();
