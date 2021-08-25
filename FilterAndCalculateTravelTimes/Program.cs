using CsvHelper;
using NetTopologySuite.Algorithm.Locate;
using NetTopologySuite.Features;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using NetTopologySuite.Operation.Buffer;
using NetTopologySuite.Precision;
using Newtonsoft.Json;
using OsmSharp.Streams;
using ProjNet.CoordinateSystems;
using ProjNet.CoordinateSystems.Transformations;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;


using OsmSharp;
using OsmSharp.Geo;
using Itinero;
using Itinero.IO.Osm;
using Itinero.Osm.Vehicles;
using Itinero.Algorithms.Networks.Analytics.Isochrones;
using System.Threading;
using NetTopologySuite.Simplify;

namespace FilterAndCalculateTravelTimes
{
    class Program
    {
        static void Main(string[] args)
        {
            string baseFolder = @"Assets";

            string RAMCode = "80G";

            string EPSG23030WKT = File.ReadAllText("ProjectionWKT/EPSG23030.txt"); // this is a pretty good North Europe projection with units in metres that is supported by ProjNet

            CoordinateSystemFactory csf = new CoordinateSystemFactory();
            CoordinateTransformationFactory trf = new CoordinateTransformationFactory();
            ICoordinateTransformation TransformToMetres = trf.CreateFromCoordinateSystems(GeographicCoordinateSystem.WGS84, csf.CreateFromWkt(EPSG23030WKT));
            ICoordinateTransformation TransformToDegrees = trf.CreateFromCoordinateSystems(csf.CreateFromWkt(EPSG23030WKT), GeographicCoordinateSystem.WGS84);

            // These settings decide what to do         

            bool filter = false;
            bool build = filter;
            bool run = filter;
            bool calculate = true;

            // Bounding box calculator https://boundingbox.klokantech.com/

            // These are our example variables and will filter our input to contain only journeys in South Yorkshire and on the 10th of September 2019.
            string dateFilterString = "2019-09-10:2019-09-11";
            // -4.0148,52.8808,0.4061,54.7738
            string minLon = "-1.8354";
            string maxLon = "-0.7571";
            string minLat = "53.1189";
            string maxLat = "53.7405";

            // These are GM boundings
            minLon = "-2.8086";
            maxLon = "-1.7759";
            minLat = "53.2318";
            maxLat = "53.839";

            // These are Scotland boundings
            minLon = "-10";
            maxLon = "10";
            minLat = "54.6";
            maxLat = "80";

            // These are GB boundings
            minLon = "-10";
            maxLon = "10";
            minLat = "40";
            maxLat = "80";

            // These are North England Zone of Influence boundings
            // minLon = "-3.7731";
            // maxLon = "0.3797";
            // minLat = "52.8258";
            // maxLat = "56.0872";

            // North England and Midlands
            maxLon = "0.509896555";
            minLon = "-3.693384901";
            maxLat = "55.811076522";
            minLat = "51.805604392";

            // These are Stranraer boundings
            //minLon = "-5.2";
            //maxLon = "-4.9";
            //minLat = "54.8";
            //maxLat = "55";

            // Trying to automate snapping MSOA centrepoints to roads, but there are problems with this library. The breaking problem is that you cannot skip malformed geographies.
            /*
            using (var fileStream = File.OpenRead("Assets/west-yorkshire-latest.osm.pbf"))
            {
                PBFOsmStreamSource source = new PBFOsmStreamSource(fileStream);
               
                var filteredfeatures = source.ToFeatureSource();
                //var allfeatures = source.Where(x => x.Type == OsmGeoType.Way).ToList().ToFeatureSource();

                while (filteredfeatures.MoveNext())                {

                    var currentFeature = filteredfeatures.Current;
                }

                // build feature collection.
                var featureCollection = new FeatureCollection();
                //var attributesTable = new AttributesTable();
                //attributesTable.Add("highway", "primary");
                
                foreach (var feature in filteredfeatures)
                {                    
                    //Console.WriteLine(feature.Geometry.GeometryType);
                    if (feature.Geometry.GeometryType == "LineString")
                    {
                        featureCollection.Add(new Feature(feature.Geometry, null));
                    }
                }
                
                GeoJsonWriter writer = new GeoJsonWriter();
                writer.SerializerSettings.Formatting = Formatting.Indented;
                File.WriteAllText($"JustHighways.geojson", writer.Write(featureCollection));
            }
            */

            /*
            var routerDb = new RouterDb();
            using (var stream = new FileInfo(@"Assets/west-yorkshire-latest.osm.pbf").OpenRead())
            {
                routerDb.LoadOsmData(stream, Vehicle.Car);
            }
            
            var router = new Router(routerDb);

            List<float> isochronetimes = new List<float> { 600f, 1200f };
            CancellationToken ct = new CancellationToken();
            Itinero.LocalGeo.Coordinate startingpoint = new Itinero.LocalGeo.Coordinate() { Latitude = 53.778775f, Longitude = -1.768858f, Elevation = 0 };

            router.CalculateIsochrones(Vehicle.Car.Fastest(), startingpoint, isochronetimes, 16, ct);

            // calculate a route.
            var route = router.Calculate(Vehicle.Car.Fastest(),
                53.778775f, -1.768858f, 53.7590148f, -1.7095111f);
            var geoJson = route.ToGeoJson();
            */

            /*
            List<MSOACentroid> MSOACentroids = new List<MSOACentroid>();
            // Load MSOA centroids to calculate the extent
            
            using (TextReader textReader = File.OpenText(@"Inputs/2011MSOA_PWC.csv"))
            {
                CsvReader csvReader = new CsvReader(textReader, CultureInfo.InvariantCulture);
                MSOACentroids = new List<MSOACentroid>(csvReader.GetRecords<MSOACentroid>());
            }

            List<string> SouthYorkshire = new List<string>()
            {
                "Doncaster",
                "Rotherham",
                "Sheffield",
                "Barnsley"
            };

            List<MSOACentroid> MSOACentroidsOfSouthYorkshire = new List<MSOACentroid>();
            foreach (string LA in SouthYorkshire)
            {
                MSOACentroidsOfSouthYorkshire.AddRange(MSOACentroids.Where(x => x.msoa11nm.StartsWith(LA)));
            }

            minLon = MSOACentroidsOfSouthYorkshire.Min(x => x.Longitude).ToString();
            maxLon = MSOACentroidsOfSouthYorkshire.Max(x => x.Longitude).ToString();
            minLat = MSOACentroidsOfSouthYorkshire.Min(x => x.Latitude).ToString();
            maxLat = MSOACentroidsOfSouthYorkshire.Max(x => x.Latitude).ToString();
            */

            if (filter == true)
            {
                // We need to crop the osm.pbf file
                // And all of the GTFS public transport files for GB
                // And then put them all in one folder, which we then use to run an open trip planner instance.
                // The output folder will be called "FilteredOutput"
                // The bounding box tool at https://boundingbox.klokantech.com/ is very useful for creating bounding boxes


                string locationString = $"{minLat}:{minLon}:{maxLat}:{maxLon}";

                // Prepare some folders for output
                if (Directory.Exists($"{baseFolder}/graphs/filtered"))
                {
                    Directory.Delete($"{baseFolder}/graphs/filtered", true);
                }
                Directory.CreateDirectory($"{baseFolder}/graphs/filtered");

                List<string> FilterCommand = new List<string>();
                FilterCommand.Add($"cd {baseFolder}");

                List<string> TimetableFiles = new List<string>();
                //TimetableFiles.Add("NPR_GTFS.zip");
                TimetableFiles.Add("GBRail_GTFS.zip");
                // TimetableFiles.Add("EA_GTFS.zip");
                TimetableFiles.Add("EM_GTFS.zip");
                // TimetableFiles.Add("L_GTFS.zip");
                TimetableFiles.Add("NE_GTFS.zip");
                TimetableFiles.Add("NW_GTFS.zip");
                //TimetableFiles.Add("S_GTFS.zip");
                // TimetableFiles.Add("SE_GTFS.zip");
                //TimetableFiles.Add("W_GTFS.zip");
                TimetableFiles.Add("WM_GTFS.zip");
                TimetableFiles.Add("Y_GTFS.zip");


                //TimetableFiles.Clear();

                // Crop the timetable files to the specificed date range and bounding box            
                foreach (string timetablefile in TimetableFiles)
                {
                    string expressionToExecute = $"java -Xmx{RAMCode} -jar gtfs-filter-0.1.jar {timetablefile} -d {dateFilterString} -l {locationString} -o {timetablefile}_tmp";
                    FilterCommand.Add(expressionToExecute);
                }

                // Crop the osm.pbf map of GB to the bounding box
                // If you are not using Windows a version of osmconvert on your platform may be available via https://wiki.openstreetmap.org/wiki/Osmconvert
                FilterCommand.Add($"osmconvert64.exe great-britain-latest.osm.pbf -b={minLon},{minLat},{maxLon},{maxLat} --complete-ways -o=graphs/filtered/gbfiltered.pbf");

                File.WriteAllLines("filtercode.bat", FilterCommand);
                Process runBatch = Process.Start("filtercode.bat");
                runBatch.WaitForExit();

                foreach (string timetablefile in TimetableFiles)
                {
                    if (File.Exists($"{baseFolder}/{timetablefile}_filtered.zip"))
                    {
                        File.Delete($"{baseFolder}/{timetablefile}_filtered.zip");
                    }
                    ZipFile.CreateFromDirectory($"{baseFolder}/{timetablefile}_tmp", $"{baseFolder}/graphs/filtered/{timetablefile}_filtered.zip", CompressionLevel.Optimal, false, Encoding.UTF8);
                    Directory.Delete($"{baseFolder}/{timetablefile}_tmp", true);
                }

                File.Copy($"{baseFolder}/build-config.json", $"{baseFolder}/graphs/filtered/build-config.json");
                File.Copy($"{baseFolder}/router-config.json", $"{baseFolder}/graphs/filtered/router-config.json");
                //File.Copy($"{baseFolder}/otp-1.5.0-SNAPSHOT-shaded.jar", $"{baseFolder}/otp-1.5.0-SNAPSHOT-shaded.jar");
            }

            if (build == true)
            {
                List<string> BuildCommand = new List<string>();
                BuildCommand.Add($"cd {baseFolder}");
                BuildCommand.Add($"java -Xmx{RAMCode} -jar otp-1.5.0-SNAPSHOT-shaded.jar --build graphs/filtered");

                File.WriteAllLines("build.bat", BuildCommand);
                Process buildBatch = Process.Start("build.bat");
                buildBatch.WaitForExit();
            }

            if (run == true)
            {
                List<string> RunCommand = new List<string>();
                RunCommand.Add($"cd {baseFolder}");
                RunCommand.Add($"java -Xmx{RAMCode} -jar otp-1.5.0-SNAPSHOT-shaded.jar --router filtered --graphs graphs --server");

                System.IO.File.WriteAllLines("run.bat", RunCommand);
                Process runBatch = Process.Start("run.bat");
                // runBatch.WaitForExit();
            }

            if (calculate == true)
            {
                /*
                // Load origins
                string originsPath = @"Inputs/ScottishGeography/ScotlandDataZoneCentroids.csv";
                ConcurrentBag<Place> Origins;
                using (TextReader textReader = File.OpenText(originsPath))
                {
                    CsvReader csvReader = new CsvReader(textReader, CultureInfo.InvariantCulture);
                    Origins = new ConcurrentBag<Place>(csvReader.GetRecords<Place>());
                }

                // Load destinations
                string destinationsPath = @"Inputs/ScottishGeography/ScotlandWorkplaceZoneCentroids.csv";
                List<Place> Destinations;
                using (TextReader textReader = File.OpenText(destinationsPath))
                {
                    CsvReader csvReader = new CsvReader(textReader, CultureInfo.InvariantCulture);
                    Destinations = new List<Place>(csvReader.GetRecords<Place>());
                }
                */

                // Load Northern MSOAs
                string NorthernMSOAPath = @"Inputs/MSOACentroids_NorthEngland_RoadSnappedLatLong.csv";
                List<MSOACentroid> NorthernMSOAs = new List<MSOACentroid>();
                using (TextReader textReader = File.OpenText(NorthernMSOAPath))
                {
                    CsvReader csvReader = new CsvReader(textReader, CultureInfo.InvariantCulture);
                    NorthernMSOAs = csvReader.GetRecords<MSOACentroid>().ToList();
                }

                ConcurrentBag<Place> Origins = new ConcurrentBag<Place>();
                List<Place> Destinations = new List<Place>();

                foreach (MSOACentroid MSOAC in NorthernMSOAs)
                {
                    Place place = new Place()
                    {
                        Latitude = MSOAC.Latitude,
                        Longitude = MSOAC.Longitude,
                        Name = MSOAC.msoa11cd
                    };
                    Origins.Add(place);
                    Destinations.Add(place);
                }


                // Acceptable modes seem to be WALK, TRANSIT, BICYCLE, CAR
                List<string> Modes = new List<string>()
                {
                    "WALK",
                    "CAR",
                    "TRANSIT,WALK",
                    "BICYCLE"
                };

                if (File.Exists("Errors.txt"))
                {
                    File.Delete("Errors.txt");
                }

                string outputfolder = "NorthernIsochrones";

                if (Directory.Exists(outputfolder) == false)
                {
                    Directory.CreateDirectory(outputfolder);
                }

                ConcurrentBag<TravelTime> TravelTimes = new ConcurrentBag<TravelTime>();
                Stopwatch sw = new Stopwatch();
                sw.Start();
                int destinationcount = 0;

                //Destinations.RemoveAll(x => x.Name != "E02003971");

                // Calculate all isochrones from Bradford
                double LeedsLat = 53.7923066; 
                double LeedsLon = -1.7523321;
                foreach (string modesstring in Modes.Where(x => x == "TRANSIT,WALK"))
                {
                    string cutoffstring = "";
                    for (int mins = 15; mins <= 180; mins = mins + 15)
                    {
                        int seconds = mins * 60;
                        cutoffstring += $"&cutoffSec={seconds}";
                    }


                    string URL = $"http://localhost:8080/otp/routers/filtered/isochrone?fromPlace={LeedsLat},{LeedsLon}&mode={modesstring}&date=09-10-2019&time=8:00am&maxWalkDistance=25000{cutoffstring}&arriveby=false";

                    HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Get, URL);
                    requestMessage.Headers.Add("Accept", "application/json"); // this makes the otp instance return geojson instead of a shapefile

                    HttpClient client = new HttpClient();
                    client.Timeout = TimeSpan.FromMinutes(60);
                    HttpResponseMessage response = client.SendAsync(requestMessage).GetAwaiter().GetResult();
                    if (response.IsSuccessStatusCode == false)
                    {
                        Console.WriteLine($"Failed during {modesstring} calculation. Skipping");
                    }
                    else
                    {
                        string responseString = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                        var reader = new NetTopologySuite.IO.GeoJsonReader();
                        FeatureCollection IsochronesForAllMinutes = reader.Read<FeatureCollection>(responseString);


                        var writer = new NetTopologySuite.IO.GeoJsonWriter();
                        writer.SerializerSettings.Formatting = Formatting.None;
                        //File.WriteAllText($"ScottishIsochrones/IsochroneBy_{modesstring}_ToWorkplaceZone_{destination.Name}_ToArriveBy_0830am_20191009_within_{minutes}minutes.geojson", writer.Write(IsochroneForThisMinute));

                        File.WriteAllText($"LeedsIsochrones_{modesstring}.geojson", writer.Write(IsochronesForAllMinutes));
                    }

                }


                Parallel.ForEach(Destinations, new ParallelOptions() { MaxDegreeOfParallelism = 7 }, (destination) =>
                {
                    //   foreach (Place destination in Destinations)
                    //   {

                    int MaxTime = 60;
                    foreach (string modesstring in Modes)
                    {
                        Console.WriteLine($"Calculating isochrones by {modesstring} to {destination.Name}.");

                        string cutoffstring = "";
                        /*
                        if (modesstring == "CAR")
                        {
                            MaxTime = 45;
                        }
                        */
                        for (int mins = 1; mins <= MaxTime; mins++)
                        {
                            int seconds = mins * 60;
                            cutoffstring += $"&cutoffSec={seconds}";
                        }
                        string URL = $"http://localhost:8080/otp/routers/filtered/isochrone?fromPlace={destination.Latitude},{destination.Longitude}&mode={modesstring}&date=09-10-2019&time=8:30am&maxWalkDistance=2500{cutoffstring}&arriveby=true";

                        //URL = @"http://localhost:8080/otp/routers/filtered/isochrone?fromPlace=54.6608467,-3.356102296&mode=WALK&date=09-10-2019&time=8:30am&maxWalkDistance=2500&cutoffSec=3600&arriveby=true";

                        HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Get, URL);
                        requestMessage.Headers.Add("Accept", "application/json"); // this makes the otp instance return geojson instead of a shapefile

                        HttpClient client = new HttpClient();
                        client.Timeout = TimeSpan.FromMinutes(10);
                        HttpResponseMessage response = client.SendAsync(requestMessage).GetAwaiter().GetResult();
                        if (response.IsSuccessStatusCode == false)
                        {
                            Console.WriteLine($"Failed during {modesstring} calculation. Skipping");
                        }
                        else
                        {
                            string responseString = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

                            var reader = new NetTopologySuite.IO.GeoJsonReader();

                            FeatureCollection IsochronesForAllMinutes = reader.Read<FeatureCollection>(responseString);
                            IFeature IsochroneForMaxTime = IsochronesForAllMinutes.Where(x => (long)x.Attributes["time"] == 60 * MaxTime).FirstOrDefault();
                            if (IsochroneForMaxTime != null && IsochroneForMaxTime.Geometry == null)
                            {
                                Console.WriteLine($"Problem with {modesstring} routing to {destination.Name}.");
                                File.AppendAllLines("Errors.txt", new string[] { $"Problem with {modesstring} routing to {destination.Name}." });
                            }
                            else
                            {
                                List<Place> AllPossibleOrigins = new List<Place>(Origins.Where(x => IsochroneForMaxTime.Geometry.EnvelopeInternal.Contains(new Point(new Coordinate() { Y = x.Latitude, X = x.Longitude }).EnvelopeInternal)));
                                ConcurrentBag<Place> OriginsStillToSearchFor = new ConcurrentBag<Place>(AllPossibleOrigins);
                                ConcurrentBag<Place> OriginsStillToSearchForTemp = new ConcurrentBag<Place>();


                                for (int minutes = 1; minutes <= MaxTime; minutes++)
                                {
                                    IFeature IsochroneForThisMinute = IsochronesForAllMinutes.Where(x => (long)x.Attributes["time"] == minutes * 60).First();

                                    if (IsochroneForThisMinute.Geometry == null)
                                    {
                                        //Console.WriteLine("Geometry was null.");
                                    }
                                    else
                                    {
                                        // Add in projection round-trip here to buffer at 50m                                        
                                        Geometry IsochroneInMetres = Transform(IsochroneForThisMinute.Geometry, (MathTransform)TransformToMetres.MathTransform);
                                        Geometry BufferedIsochroneInMetres = new BufferOp(IsochroneInMetres).GetResultGeometry(100);
                                        Geometry BufferedIsochroneInDegrees = Transform(BufferedIsochroneInMetres, (MathTransform)TransformToDegrees.MathTransform);

                                        BufferedIsochroneInDegrees = GeometryPrecisionReducer.Reduce(BufferedIsochroneInDegrees, new PrecisionModel(10000));


                                        if (minutes % 15 == 0)
                                        {
                                            var writer = new NetTopologySuite.IO.GeoJsonWriter();
                                            writer.SerializerSettings.Formatting = Formatting.None;
                                            //File.WriteAllText($"ScottishIsochrones/IsochroneBy_{modesstring}_ToWorkplaceZone_{destination.Name}_ToArriveBy_0830am_20191009_within_{minutes}minutes.geojson", writer.Write(IsochroneForThisMinute));

                                            File.WriteAllText($"{outputfolder}/Buffered100m_IsochroneBy_{modesstring}_ToWorkplaceZone_{destination.Name}_ToArriveBy_0830am_20191009_within_{minutes}minutes.geojson", writer.Write(BufferedIsochroneInDegrees));
                                        }

                                        // This is much quicker than other methods because it indexes the geometry for the interior/exterior test
                                        IndexedPointInAreaLocator ipal = new IndexedPointInAreaLocator(BufferedIsochroneInDegrees);

                                        //ConcurrentBag<Place> OriginsStillToSearchFor = Origins.Where(x => TravelTimes.ToList().Exists(y => y.OriginName == x.Name && y.DestinationName == destination.Name && y.Mode == modesstring && y.Minutes != 0 ) == false).ToList();

                                        int foundcount = AllPossibleOrigins.Count - OriginsStillToSearchFor.Count;
                                        // Console.WriteLine($"Searching within isochrone by {modesstring} from {destination.Name} in {minutes} minutes. Journey times found for {foundcount} of {AllPossibleOrigins.Count} possible origins.");

                                        foreach (var origin in OriginsStillToSearchFor)
                                        {
                                            Point testpoint = new Point(new Coordinate() { Y = origin.Latitude, X = origin.Longitude });
                                            //bool contains = IsochroneForThisMinute.Geometry.Contains(testpoint);

                                            Location loc = ipal.Locate(new Coordinate() { Y = origin.Latitude, X = origin.Longitude });

                                            if (loc == Location.Interior)
                                            {
                                                TravelTime travelTime = new TravelTime()
                                                {
                                                    OriginName = origin.Name,
                                                    OriginLatitute = origin.Latitude,
                                                    OriginLongitude = origin.Longitude,
                                                    DestinationName = destination.Name,
                                                    DestinationLatitute = destination.Latitude,
                                                    DestinationLongitude = destination.Longitude,
                                                    Mode = modesstring,
                                                    Minutes = minutes
                                                };
                                                TravelTimes.Add(travelTime);
                                            }
                                            else
                                            {
                                                OriginsStillToSearchForTemp.Add(origin);
                                            }

                                        }
                                        OriginsStillToSearchFor = new ConcurrentBag<Place>(OriginsStillToSearchForTemp);
                                        OriginsStillToSearchForTemp.Clear();
                                    }
                                }
                            }
                        }
                    }

                    Console.WriteLine($"Completed {destinationcount++} of {Destinations.Count} in {Math.Round(sw.Elapsed.TotalMinutes, 0)} minutes. Estimated time left is {Math.Round((Destinations.Count * sw.Elapsed.TotalHours / destinationcount) - sw.Elapsed.TotalHours, 0)} hours.");
                });

                // Write out results
                using (TextWriter textWriter = File.CreateText(@"TravelTimesScotland_DataZonesToWorkplaceZones__ToArriveBy_0830am_20191009.csv"))
                {
                    CsvWriter CSVwriter = new CsvWriter(textWriter, CultureInfo.InvariantCulture);
                    CSVwriter.WriteRecords(TravelTimes);
                }
            }
        }


        public static Geometry Transform(Geometry geom, MathTransform transform)
        {
            geom = geom.Copy();
            geom.Apply(new MTF(transform));
            return geom;
        }
        sealed class MTF : NetTopologySuite.Geometries.ICoordinateSequenceFilter
        {
            private readonly MathTransform _mathTransform;

            public MTF(MathTransform mathTransform) => _mathTransform = mathTransform;

            public bool Done => false;
            public bool GeometryChanged => true;
            public void Filter(CoordinateSequence seq, int i)
            {
                double x = seq.GetX(i);
                double y = seq.GetY(i);
                double[] transformed = _mathTransform.Transform(new double[] { x, y });
                seq.SetX(i, transformed[0]);
                seq.SetY(i, transformed[1]);
            }
        }
    }
    public class MSOACentroid
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        public string msoa11cd { get; set; }
        public string msoa11nm { get; set; }
    }

    public class TravelTime
    {
        public string OriginName { get; set; }
        public double OriginLatitute { get; set; }
        public double OriginLongitude { get; set; }
        public string DestinationName { get; set; }
        public double DestinationLatitute { get; set; }
        public double DestinationLongitude { get; set; }
        public string Mode { get; set; }
        public int Minutes { get; set; }
    }

    public class Place
    {
        public string Name { get; set; }
        public string masterpc { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

}