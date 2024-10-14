const { MongoClient, ObjectId } = require("mongodb");
const fs = require("fs");

// MongoDB Connection URI
const uri = "mongodb://localhost:27017";
const dbName = "check_db";

// Sample sleep data to insert into the sleep_data collection
const sleepData = [
  {
    user: "A",
    day: "Friday",
    date: new Date("2022-04-01"),
    sleep_score: 90,
    hours_of_sleep: "7:22:00",
    rem_sleep: "18%",
    deep_sleep: "21%",
    heart_rate_below_resting: "98%",
    duration_in_bed: "9:49pm - 6:01am",
    hours_in_bed: "?",
  },
  {
    user: "A",
    day: "Saturday",
    date: new Date("2022-04-02"),
    sleep_score: 89,
    hours_of_sleep: "8:40:00",
    rem_sleep: "21%",
    deep_sleep: "21%",
    heart_rate_below_resting: "73%",
    duration_in_bed: "9:50pm - 7:26am",
    hours_in_bed: "?",
  },
  {
    user: "A",
    day: "Sunday",
    date: new Date("2022-04-03"),
    sleep_score: 81,
    hours_of_sleep: "8:52:00",
    rem_sleep: "21%",
    deep_sleep: "17%",
    heart_rate_below_resting: "26%",
    duration_in_bed: "11:29pm - 9:54am",
    hours_in_bed: "?",
  },
  {
    user: "A",
    day: "Monday",
    date: new Date("2022-04-04"),
    sleep_score: 83,
    hours_of_sleep: "6:50:00",
    rem_sleep: "17%",
    deep_sleep: "19%",
    heart_rate_below_resting: "99%",
    duration_in_bed: "10:12pm - 5:49am",
    hours_in_bed: "?",
  },
  {
    user: "A",
    day: "Tuesday",
    date: new Date("2022-04-05"),
    sleep_score: 84,
    hours_of_sleep: "6:57:00",
    rem_sleep: "18%",
    deep_sleep: "21%",
    heart_rate_below_resting: "97%",
    duration_in_bed: "9:45pm - 5:43am",
    hours_in_bed: "?",
  },
  {
    user: "A",
    day: "Wednesday",
    date: new Date("2022-04-06"),
    sleep_score: 83,
    hours_of_sleep: "7:27:00",
    rem_sleep: "17%",
    deep_sleep: "19%",
    heart_rate_below_resting: "77%",
    duration_in_bed: "9:22pm - 6:14am",
    hours_in_bed: "?",
  },
  {
    user: "A",
    day: "Thursday",
    date: new Date("2022-04-07"),
    sleep_score: 87,
    hours_of_sleep: "7:57:00",
    rem_sleep: "22%",
    deep_sleep: "14%",
    heart_rate_below_resting: "68%",
    duration_in_bed: "10:05pm - 6:55am",
    hours_in_bed: "?",
  },
  {
    user: "B",
    day: "Friday",
    date: new Date("2022-04-01"),
    sleep_score: 83,
    hours_of_sleep: "7:27:00",
    rem_sleep: "19%",
    deep_sleep: "18%",
    heart_rate_below_resting: "71%",
    duration_in_bed: "9:42pm - 6:36am",
    hours_in_bed: "?",
  },
  {
    user: "B",
    day: "Saturday",
    date: new Date("2022-04-02"),
    sleep_score: 87,
    hours_of_sleep: "8:12:00",
    rem_sleep: "19%",
    deep_sleep: "15%",
    heart_rate_below_resting: "71%",
    duration_in_bed: "11:27pm - 8:36am",
    hours_in_bed: "?",
  },
  {
    user: "B",
    day: "Sunday",
    date: new Date("2022-04-03"),
    sleep_score: 83,
    hours_of_sleep: "6:57:00",
    rem_sleep: "15%",
    deep_sleep: "18%",
    heart_rate_below_resting: "96%",
    duration_in_bed: "12:53am - 8:39am",
    hours_in_bed: "?",
  },
  {
    user: "B",
    day: "Monday",
    date: new Date("2022-04-04"),
    sleep_score: 88,
    hours_of_sleep: "7:39:00",
    rem_sleep: "17%",
    deep_sleep: "19%",
    heart_rate_below_resting: "98%",
    duration_in_bed: "9:35pm - 6:02am",
    hours_in_bed: "?",
  },
  {
    user: "B",
    day: "Tuesday",
    date: new Date("2022-04-05"),
    sleep_score: 86,
    hours_of_sleep: "8:00:00",
    rem_sleep: "20%",
    deep_sleep: "19%",
    heart_rate_below_resting: "73%",
    duration_in_bed: "9:52pm - 7:10am",
    hours_in_bed: "?",
  },
  {
    user: "B",
    day: "Wednesday",
    date: new Date("2022-04-06"),
    sleep_score: 82,
    hours_of_sleep: "7:48:00",
    rem_sleep: "11%",
    deep_sleep: "13%",
    heart_rate_below_resting: "73%",
    duration_in_bed: "10:51pm - 8:02am",
    hours_in_bed: "?",
  },
  {
    user: "B",
    day: "Thursday",
    date: new Date("2022-04-07"),
    sleep_score: 83,
    hours_of_sleep: "7:19:00",
    rem_sleep: "19%",
    deep_sleep: "23%",
    heart_rate_below_resting: "77%",
    duration_in_bed: "9:35pm - 5:58am",
    hours_in_bed: "?",
  },
];

const activityData = [
  {
    user: "A",
    date: new Date("2022-04-01"),
    start_time: "12:21:10 PM",
    end_time: "1:47:22 PM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 5768,
    distance: 0,
    elevation_gain: 0,
    calories: 508,
  },
  {
    user: "A",
    date: new Date("2022-04-01"),
    start_time: "6:01:08 PM",
    end_time: "6:37:14 PM",
    activity: "Run",
    log_type: "tracker",
    steps: 5760,
    distance: 6.465203,
    elevation_gain: 46.33,
    calories: 460,
  },
  {
    user: "B",
    date: new Date("2022-04-01"),
    start_time: "11:27:42 AM",
    end_time: "12:58:28 PM",
    activity: "Hike",
    log_type: "tracker",
    steps: 6519,
    distance: 4.46707,
    elevation_gain: 188.976,
    calories: 730,
  },
  {
    user: "A",
    date: new Date("2022-04-02"),
    start_time: "12:26:20 PM",
    end_time: "1:25:13 PM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 4812,
    distance: 0,
    elevation_gain: 91.44,
    calories: 343,
  },
  {
    user: "B",
    date: new Date("2022-04-02"),
    start_time: "1:53:22 PM",
    end_time: "2:39:28 PM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 4147,
    distance: 0,
    elevation_gain: 243.84,
    calories: 357,
  },
  {
    user: "A",
    date: new Date("2022-04-03"),
    start_time: "8:30:12 AM",
    end_time: "8:48:08 AM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 1640,
    distance: 0,
    elevation_gain: 15.24,
    calories: 141,
  },
  {
    user: "B",
    date: new Date("2022-04-03"),
    start_time: "6:01:08 PM",
    end_time: "6:37:14 PM",
    activity: "Run",
    log_type: "tracker",
    steps: 5760,
    distance: 6.465203,
    elevation_gain: 46.33,
    calories: 460,
  },
  {
    user: "A",
    date: new Date("2022-04-04"),
    start_time: "10:24:12 AM",
    end_time: "10:30:22 AM",
    activity: "Fitstar: Personal Trainer",
    log_type: "fitstar",
    steps: 117,
    distance: 0,
    elevation_gain: 0,
    calories: 45,
  },
  {
    user: "B",
    date: new Date("2022-04-04"),
    start_time: "7:32:36 AM",
    end_time: "7:52:14 AM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 2065,
    distance: 0,
    elevation_gain: 6.096,
    calories: 171,
  },
  {
    user: "A",
    date: new Date("2022-04-05"),
    start_time: "7:37:22 AM",
    end_time: "8:01:15 AM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 2369,
    distance: 0,
    elevation_gain: 30.48,
    calories: 262,
  },
  {
    user: "B",
    date: new Date("2022-04-05"),
    start_time: "8:50:13 PM",
    end_time: "9:08:09 PM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 1573,
    distance: 0,
    elevation_gain: 6.096,
    calories: 134,
  },
  {
    user: "A",
    date: new Date("2022-04-06"),
    start_time: "3:02:08 PM",
    end_time: "3:17:30 PM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 1393,
    distance: 0,
    elevation_gain: 6.096,
    calories: 115,
  },
  {
    user: "B",
    date: new Date("2022-04-06"),
    start_time: "8:23:23 AM",
    end_time: "8:48:59 AM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 1779,
    distance: 0,
    elevation_gain: 15.24,
    calories: 166,
  },
  {
    user: "A",
    date: new Date("2022-04-07"),
    start_time: "11:46:26 AM",
    end_time: "12:03:30 PM",
    activity: "Walk",
    log_type: "auto_detected",
    steps: 1536,
    distance: 0,
    elevation_gain: 9.144,
    calories: 133,
  },
  {
    user: "B",
    date: new Date("2022-04-07"),
    start_time: "9:39:00 AM",
    end_time: "10:15:46 AM",
    activity: "Run",
    log_type: "tracker",
    steps: 5937,
    distance: 6.47782,
    elevation_gain: 45.415,
    calories: 472,
  },
];

//   async function insertActivityData(client) {
//     try {
//       const db = client.db(dbName);
//       const activityCollection = db.collection('activity_data');

//       await activityCollection.insertMany(activityData);
//       console.log('Activity data inserted successfully!');

//     } catch (err) {
//       console.error('Error inserting activity data:', err);
//     }
//   }

const users = [
  {
    _id: new ObjectId("61a7345d1c4ae1c58d123456"),
    name: "Brad",
    timezone: "Americas/Los Angeles",
    version: 70,
    app: "Wysa",
    country: "US",
    createdAt: new Date("2021-11-18T15:56:11.553Z"),
    updatedAt: new Date("2021-11-18T15:56:46.392Z"),
  },
  {
    _id: new ObjectId("61a7345d1c4ae1c58d654321"),
    name: "Alice",
    timezone: "Europe/London",
    version: 71,
    app: "Wysa",
    country: "UK",
    createdAt: new Date("2021-12-01T12:45:11.553Z"),
    updatedAt: new Date("2021-12-01T12:45:46.392Z"),
  },
  {
    _id: new ObjectId("61b7345d1c4ae1c58d789012"),
    name: "Charlie",
    timezone: "Asia/Kolkata",
    version: 72,
    app: "Wysa",
    country: "India",
    createdAt: new Date("2022-01-10T10:12:30.123Z"),
    updatedAt: new Date("2022-01-10T10:15:46.789Z"),
  },
  {
    _id: new ObjectId("61c7345d1c4ae1c58d987654"),
    name: "Diana",
    timezone: "Europe/Berlin",
    version: 73,
    app: "Wysa",
    country: "Germany",
    createdAt: new Date("2022-02-20T09:45:11.553Z"),
    updatedAt: new Date("2022-02-20T09:48:46.392Z"),
  },
  {
    _id: new ObjectId("61d7345d1c4ae1c58d543210"),
    name: "Edward",
    timezone: "Americas/New York",
    version: 74,
    app: "Wysa",
    country: "US",
    createdAt: new Date("2022-03-05T12:30:25.123Z"),
    updatedAt: new Date("2022-03-05T12:32:46.789Z"),
  },
];

const moods = [
  {
    _id: new ObjectId(),
    field: "mood_score",
    user: users[0]._id, // Refers to user Brad
    value: 8, // Mood score is between 1-10
    createdAt: new Date("2022-04-01T11:24:25.466Z"),
    updatedAt: new Date("2022-04-01T11:24:25.466Z"),
  },
  {
    _id: new ObjectId(),
    field: "mood_score",
    user: users[1]._id, // Refers to user Alice
    value: 7, // Mood score is between 1-10
    createdAt: new Date("2022-04-02T12:15:35.466Z"),
    updatedAt: new Date("2022-04-02T12:15:35.466Z"),
  },
  {
    _id: new ObjectId(),
    field: "mood_score",
    user: users[2]._id, // Refers to user Charlie
    value: 9, // Mood score is between 1-10
    createdAt: new Date("2022-04-03T13:10:45.466Z"),
    updatedAt: new Date("2022-04-03T13:10:45.466Z"),
  },
  {
    _id: new ObjectId(),
    field: "mood_score",
    user: users[3]._id, // Refers to user Diana
    value: 6, // Mood score is between 1-10
    createdAt: new Date("2022-04-04T14:20:55.466Z"),
    updatedAt: new Date("2022-04-04T14:20:55.466Z"),
  },
  {
    _id: new ObjectId(),
    field: "mood_score",
    user: users[4]._id, // Refers to user Edward
    value: 7, // Mood score is between 1-10
    createdAt: new Date("2022-04-05T15:30:05.466Z"),
    updatedAt: new Date("2022-04-05T15:30:05.466Z"),
  },
];

// Helper function to insert data into a given collection
async function insertData(client, collectionName, data) {
  try {
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    const result = await collection.insertMany(data);
    console.log(
      `${result.insertedCount} records inserted into ${collectionName} collection.`
    );
  } catch (err) {
    console.error(`Error inserting data into ${collectionName}:`, err);
  }
}

// Insert all collections
async function insertAllData(client) {
  await insertData(client, "sleep_data", sleepData);
  await insertData(client, "activity_data", activityData);
  await insertData(client, "users", users);
  await insertData(client, "moods", moods);
}

// Function to run the aggregation pipeline
async function fetchUserActivity() {
  const client = new MongoClient(uri);

  try {
    await client.connect();

    console.log("Connected successfully to MongoDB");

    await insertAllData(client); // Insert sleep, activity, users, and mood data

    const db = client.db(dbName);
    const users = db.collection("users");

    // Aggregation pipeline
    const results = await users
      .aggregate([
        {
          $lookup: {
            from: "mood",
            localField: "_id",
            foreignField: "user",
            as: "mood_data",
          },
        },
        {
          $lookup: {
            from: "activity",
            localField: "_id",
            foreignField: "user",
            as: "activity_data",
          },
        },
        {
          $lookup: {
            from: "sleep",
            localField: "_id",
            foreignField: "user",
            as: "sleep_data",
          },
        },
        {
          $addFields: {
            mood_score: {
              $arrayElemAt: [
                {
                  $filter: {
                    input: "$mood_data",
                    as: "mood",
                    cond: {
                      $eq: [
                        {
                          $dateToString: {
                            format: "%Y-%m-%d",
                            date: "$$mood.createdAt",
                          },
                        },
                        "2022-04-01",
                      ],
                    },
                  },
                },
                0,
              ],
            },
            activity_on_date: {
              $filter: {
                input: "$activity_data",
                as: "activity",
                cond: {
                  $eq: [
                    {
                      $dateToString: {
                        format: "%Y-%m-%d",
                        date: "$$activity.StartTime",
                      },
                    },
                    "2022-04-01",
                  ],
                },
              },
            },
            sleep_on_date: {
              $filter: {
                input: "$sleep_data",
                as: "sleep",
                cond: {
                  $eq: [
                    {
                      $dateToString: {
                        format: "%Y-%m-%d",
                        date: "$$sleep.date",
                      },
                    },
                    "2022-04-01",
                  ],
                },
              },
            },
          },
        },
        {
          $project: {
            _id: 1,
            name: 1,
            date: "2022-04-01",
            mood_score: "$mood_score.value",
            activity: {
              $map: {
                input: "$activity_on_date",
                as: "activity",
                in: {
                  activity: "$$activity.Activity",
                  steps: "$$activity.Steps",
                  distance: "$$activity.Distance",
                  duration: {
                    $divide: [
                      {
                        $subtract: [
                          "$$activity.EndTime",
                          "$$activity.StartTime",
                        ],
                      },
                      60000,
                    ],
                  },
                },
              },
            },
            sleep: {
              $arrayElemAt: [
                {
                  $map: {
                    input: "$sleep_on_date",
                    as: "sleep",
                    in: {
                      sleep_score: "$$sleep.sleep_score",
                      hours_of_sleep: "$$sleep.hours_of_sleep",
                      hours_in_bed: "$$sleep.hours_in_bed",
                    },
                  },
                },
                0,
              ],
            },
          },
        },
      ])
      .toArray();

    // Write result to JSON file
    fs.writeFileSync(
      "user_activity_data.json",
      JSON.stringify(results, null, 2)
    );
    console.log("Data written to user_activity_data.json");
  } catch (err) {
    console.error("Error fetching user activity data:", err);
  } finally {
    await client.close();
  }
}

// Run the batch process
fetchUserActivity();
