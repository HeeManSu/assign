

const { MongoClient } = require("mongodb");
const fs = require("fs");

const uri = "mongodb://localhost:27017"; 
const dbName = "assignDB"; 

const sleepData = [
  {
    user: "A",
    SleepScore: 90,
    HoursOfSleep: "07:22:00",
    DurationInBedPart1: new Date("2022-04-01T21:49:00Z"), 
    DurationInBedPart2: new Date("2022-04-02T06:01:00Z"), 
  },
  {
    user: "A",
    SleepScore: 89,
    HoursOfSleep: "08:40:00",
    DurationInBedPart1: new Date("2022-04-02T21:50:00Z"), 
    DurationInBedPart2: new Date("2022-04-03T07:26:00Z"), 
  },
  {
    user: "A",
    SleepScore: 81,
    HoursOfSleep: "08:52:00",
    DurationInBedPart1: new Date("2022-04-03T23:29:00Z"), 
    DurationInBedPart2: new Date("2022-04-04T09:54:00Z"), 
  },
  {
    user: "A",
    SleepScore: 83,
    HoursOfSleep: "06:50:00",
    DurationInBedPart1: new Date("2022-04-04T22:12:00Z"), 
    DurationInBedPart2: new Date("2022-04-05T05:49:00Z"), 
  },
  {
    user: "A",
    SleepScore: 84,
    HoursOfSleep: "06:57:00",
    DurationInBedPart1: new Date("2022-04-05T21:45:00Z"), 
    DurationInBedPart2: new Date("2022-04-06T05:43:00Z"), 
  },
  {
    user: "A",
    SleepScore: 83,
    HoursOfSleep: "07:27:00",
    DurationInBedPart1: new Date("2022-04-06T21:22:00Z"), 
    DurationInBedPart2: new Date("2022-04-07T06:14:00Z"), 
  },
  {
    user: "A",
    SleepScore: 87,
    HoursOfSleep: "07:57:00",
    DurationInBedPart1: new Date("2022-04-07T22:05:00Z"), 
    DurationInBedPart2: new Date("2022-04-08T06:55:00Z"), 
  },
  {
    user: "B",
    SleepScore: 83,
    HoursOfSleep: "07:27:00",
    DurationInBedPart1: new Date("2022-04-01T21:42:00Z"), 
    DurationInBedPart2: new Date("2022-04-02T06:36:00Z"), 
  },
  {
    user: "B",
    SleepScore: 87,
    HoursOfSleep: "08:12:00",
    DurationInBedPart1: new Date("2022-04-02T23:27:00Z"), 
    DurationInBedPart2: new Date("2022-04-03T08:36:00Z"), 
  },
  {
    user: "B",
    SleepScore: 83,
    HoursOfSleep: "06:57:00",
    DurationInBedPart1: new Date("2022-04-03T00:53:00Z"), 
    DurationInBedPart2: new Date("2022-04-03T08:39:00Z"), 
  },
  {
    user: "B",
    SleepScore: 88,
    HoursOfSleep: "07:39:00",
    DurationInBedPart1: new Date("2022-04-04T21:35:00Z"), 
    DurationInBedPart2: new Date("2022-04-05T06:02:00Z"), 
  },
  {
    user: "B",
    SleepScore: 86,
    HoursOfSleep: "08:00:00",
    DurationInBedPart1: new Date("2022-04-05T21:52:00Z"),
    DurationInBedPart2: new Date("2022-04-06T07:10:00Z"),
  },
  {
    user: "B",
    SleepScore: 82,
    HoursOfSleep: "07:48:00",
    DurationInBedPart1: new Date("2022-04-06T22:51:00Z"),
    DurationInBedPart2: new Date("2022-04-07T08:02:00Z"),
  },
  {
    user: "B",
    SleepScore: 83,
    HoursOfSleep: "07:19:00",
    DurationInBedPart1: new Date("2022-04-07T21:35:00Z"),
    DurationInBedPart2: new Date("2022-04-08T05:58:00Z"),
  },
];

const activityData = [
  {
    user: "a",
    Activity: "run",
    Steps: 6250,
    Distance: 5,
    StartTime: new Date("2022-04-01T14:00:00Z"),
    EndTime: new Date("2022-04-01T14:42:00Z"),
  },
  {
    user: "a",
    Activity: "swim",
    Distance: 200,
    StartTime: new Date("2022-04-01T16:00:00Z"),
    EndTime: new Date("2022-04-01T16:40:00Z"),
  },
  {
    user: "a",
    Activity: "walk",
    Steps: 3000,
    Distance: 2,
    StartTime: new Date("2022-04-01T18:00:00Z"),
    EndTime: new Date("2022-04-01T18:30:00Z"),
  },
  {
    user: "b",
    Activity: "run",
    Steps: 7000,
    Distance: 6,
    StartTime: new Date("2022-04-01T15:00:00Z"),
    EndTime: new Date("2022-04-01T15:45:00Z"),
  },
];

const users = [
  { _id: "a", name: "Brad", date: new Date("2022-04-01") },
  { _id: "b", name: "Alice", date: new Date("2022-04-01") },
  { _id: "c", name: "Charlie", date: new Date("2022-04-01") },
  { _id: "d", name: "Diana", date: new Date("2022-04-01") },
  { _id: "e", name: "Ethan", date: new Date("2022-04-01") },
];

const moods = [
  { user: "a", value: 8, createdAt: new Date("2022-04-01T11:24:25.466Z") },
  { user: "a", value: 7, createdAt: new Date("2022-04-01T12:00:00.000Z") },

  { user: "b", value: 9, createdAt: new Date("2022-04-01T11:24:25.466Z") },
  { user: "b", value: 6, createdAt: new Date("2022-04-01T12:00:00.000Z") },

  { user: "c", value: 7, createdAt: new Date("2022-04-01T11:24:25.466Z") },
  { user: "c", value: 5, createdAt: new Date("2022-04-01T12:00:00.000Z") },

  { user: "d", value: 6, createdAt: new Date("2022-04-01T11:24:25.466Z") },
  { user: "d", value: 8, createdAt: new Date("2022-04-01T12:00:00.000Z") },

  { user: "e", value: 8, createdAt: new Date("2022-04-01T11:24:25.466Z") },
  { user: "e", value: 7, createdAt: new Date("2022-04-01T12:00:00.000Z") },
];

async function insertData(client, collectionName, data) {
  try {
    const result = await client
      .db(dbName)
      .collection(collectionName)
      .insertMany(data);
    console.log(
      `${result.insertedCount} documents inserted into ${collectionName}`
    );
  } catch (error) {
    console.error(`Error inserting data into ${collectionName}:`, error);
  }
}

async function insertAllData(client) {
  await insertData(client, "sleep", sleepData);
  await insertData(client, "activities", activityData);
  await insertData(client, "users", users);
  await insertData(client, "moods", moods);
}

const aggregationPipeline = [
  {
    $match: {
      date: {
        $gte: new Date("2022-04-01"),
        $lt: new Date("2022-04-02"),
      },
    },
  },
  {
    $lookup: {
      from: "moods",
      localField: "_id",
      foreignField: "user",
      as: "mood",
    },
  },
  {
    $lookup: {
      from: "activities",
      localField: "_id",
      foreignField: "user",
      as: "activity",
    },
  },
  {
    $lookup: {
      from: "sleep",
      localField: "_id",
      foreignField: "user",
      as: "sleep",
    },
  },
  {
    $project: {
      user: "$_id",
      _id: 0,
      date: 1,
      mood_score: { $arrayElemAt: ["$mood.value", 0] },
      activity: {
        $map: {
          input: "$activity",
          as: "act",
          in: {
            activity: "$$act.Activity",
            steps: "$$act.Steps",
            distance: "$$act.Distance",
            duration: { $subtract: ["$$act.EndTime", "$$act.StartTime"] },
          },
        },
      },
      sleep: {
        $let: {
          vars: {
            sleepData: { $arrayElemAt: ["$sleep", 0] },
          },
          in: {
            sleep_score: "$$sleepData.SleepScore",
            hours_of_sleep: "$$sleepData.HoursOfSleep",
            hours_in_bed: {
              $subtract: [
                "$$sleepData.DurationInBedPart2",
                "$$sleepData.DurationInBedPart1",
              ],
            },
          },
        },
      },
    },
  },
];

async function fetchUserActivity() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const database = client.db(dbName);

    await insertAllData(client);

    const usersCollection = database.collection("users");

    const result = await usersCollection
      .aggregate(aggregationPipeline)
      .toArray();

    fs.writeFileSync(
      "user_activity_data.json",
      JSON.stringify(result, null, 2)
    );
    console.log("Data written to user_activity_data.json");
  } catch (error) {
    console.error("Error:", error);
  } finally {
    await client.close();
    console.log("Disconnected from MongoDB");
  }
}

fetchUserActivity();
