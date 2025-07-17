-- creating dimVisitor table

CREATE TABLE dimVisitor (
    VisitorKey INT IDENTITY(1,1),
    VisitorID INT,
    FirstVisitDate DATETIME,
    LastVisitDate DATETIME,
    TotalViews INT,  
    TotalAddToCarts INT,
    TotalPurchases INT,   
    ConversionRate DECIMAL(5,4),
    TotalEvents INT,
    IsValidFunnel BIT,
    CONSTRAINT PK_dimVisitor PRIMARY KEY NONCLUSTERED (VisitorKey) NOT ENFORCED
);

--copying data into dimVisitor table
COPY INTO dimVisitor (
    VisitorID,
    FirstVisitDate,
    LastVisitDate,
    TotalViews,
    TotalAddToCarts,
    TotalPurchases,
    ConversionRate,
    TotalEvents,
    IsValidFunnel
)
FROM 'https://project1azure1.dfs.core.windows.net/gold/parquet/dim_visitor'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
);

--creating dimTime table
CREATE TABLE dimTime(
Hour INT,
Minute INT,
TimeKey INT NOT NULL,
TimeOfDay VARCHAR(10),
IsBussinessHours INT,
CONSTRAINT PK_dimTime PRIMARY KEY NONCLUSTERED(TimeKey) NOT ENFORCED
)

--copying data into dimTime

COPY INTO dimTime 
FROM 'https://project1azure1.dfs.core.windows.net/gold/parquet/dim_time'
WITH(
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY= 'Managed Identity')
)

--creating dimItem table
CREATE TABLE dimItem(
ItemKey INT IDENTITY(1,1),
itemid INT,
total_views INT,
total_addtocarts INT,
total_purchases INT,
current_category_id INT,
current_availability INT,
Conversion_rate FLOAT,
latest_timestamp DATETIME,
CONSTRAINT PK_dimItem PRIMARY KEY NONCLUSTERED(ItemKey) NOT ENFORCED
)

--copying data to dimItem
COPY INTO dimItem(
itemid,
total_views,
total_addtocarts,
total_purchases,
current_category_id,
current_availability,
Conversion_rate,
latest_timestamp 
)
FROM 'https://project1azure1.dfs.core.windows.net/gold/parquet/dim_item'
WITH(
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
)

--creating dimEventType table
CREATE TABLE dimEventType(
EventTypeKey INT,
EventName VARCHAR(20),
CONSTRAINT PK_dimEventType PRIMARY KEY NONCLUSTERED(EventTypeKey) NOT ENFORCED
)


--copying data into dimEventType table
COPY INTO dimEventType
FROM 'https://project1azure1.dfs.core.windows.net/gold/parquet/dim_event_type'
WITH(
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
)


--creating dimDate table
CREATE TABLE dimDate(
event_date DATE,
DateKey INT,
Year INT,
Month INT,
Day INT,
Quarter INT,
DayOfWeek INT,
WeekOfYear INT,
IsWeekend BIT,
CONSTRAINT PK_dimDate PRIMARY KEY NONCLUSTERED(DateKey) NOT ENFORCED
)

--copying data into dimDate table

COPY INTO dimDate
FROM 'https://project1azure1.dfs.core.windows.net/gold/parquet/dim_date'
WITH(
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
)



--creating dimCategory table
CREATE TABLE dimCategory(
CategoryKey INT IDENTITY(1,1),
categoryid INT,
parentid INT,
CategoryLevel INT,
IsLeafCategory INT,
top_categoryid INT,
CONSTRAINT PK_dimCategory PRIMARY KEY NONCLUSTERED(CategoryKey) NOT ENFORCED
)


--copying data into dimCategory table
COPY INTO dimCategory(
categoryid,
parentid,
CategoryLevel,
IsLeafCategory,
top_categoryid
)
FROM 'https://project1azure1.dfs.core.windows.net/gold/parquet/dim_category'
WITH(
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
)


--creating FactBehaviorEvents table

CREATE TABLE FactBehaviorEvents (
    EventKey BIGINT IDENTITY(1,1),          
    VisitorKey INT NOT NULL,                -- FK from dimVisitor
    ItemKey INT NOT NULL,                   -- FK from dimItem
    DateKey INT NOT NULL,                   -- FK from dimDate
    TimeKey INT NOT NULL,                   -- FK from dimTime
    EventTypeKey INT NOT NULL,              -- FK from dimEventType
    transactionid INT,
    CONSTRAINT PK_FactBehaviorEvents PRIMARY KEY NONCLUSTERED (EventKey) NOT ENFORCED
);

--creating Events table

CREATE TABLE Events (
    timestamp DATETIME2,
    visitorid INT,
    event VARCHAR(20),
    itemid INT,
    transactionid INT
);


--copying data into Events table
COPY INTO Events
FROM 'https://project1azure1.dfs.core.windows.net/gold/parquet/events'
WITH(
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
)


--inserting data into FactBehaviorEvents
INSERT INTO FactBehaviorEvents(
    VisitorKey,
    ItemKey,
    DateKey,
    TimeKey,
    EventTypeKey,
    transactionid
)
SELECT dv.VisitorKey,di.ItemKey,dd.DateKey,dt.TimeKey,det.EventTypeKey,e.transactionid FROM Events e
JOIN dimVisitor dv ON e.visitorid = dv.VisitorID
JOIN dimItem di ON e.itemid = di.itemid
JOIN dimEventType det ON e.event = det.EventName
CROSS APPLY(
    SELECT
        CAST(e.timestamp as DATE) as EventDate,
        DATEPART(HOUR,e.timestamp) as EventHour,
        DATEPART(MINUTE,e.timestamp) as EventMinute
) t 
JOIN dimDate dd ON t.Eventdate = dd.event_date
JOIN dimTime dt on t.EventHour = dt.Hour and t.EventMinute = dt.Minute






