using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;

namespace Axio.Stuff.CosmosDbSample
{
    class GremlinQuery
    {
        public GremlinQuery(string description, string statement)
        {
            Description = description;
            Statement = statement;
        }

        public string Description { get; private set; }

        public string Statement { get; private set; }
    }

    class Program
    {
        private static string hostname = "yourhostname.gremlin.cosmosdb.azure.com";
        private static int port = 443;
        private static string authKey = "yourAuthKey";
        private static string database = "yourGraphDbName";
        private static string collection = "yourGraph";

        static async Task Main(string[] args)
        {
            string[] names = {
                "Hazel", "Madeline", "Isaac", "Shelia", "Christy", "Thelma", "Kara", "Johnnie", "Ron", "Frances",
                "Eddie", "Mona", "Jose", "Santos", "Joyce", "Jim", "Shannon", "Ralph", "Benjamin", "Estelle", "Celia"
            };

            var gremlinServer = new GremlinServer(
                hostname, port, enableSsl: true,
                username: "/dbs/" + database + "/colls/" + collection,
                password: authKey);

            var queries = ConstructSociogram(names);
            await ExecuteGraphQueriesAsync(gremlinServer, queries);

            Console.WriteLine("Graph constructed. B-)");
            Console.ReadLine();
        }

        private static IEnumerable<GremlinQuery> ConstructSociogram(string[] names)
        {
            var queries = new List<GremlinQuery>();

            var rand = new Random();
            var vertexCount = names.Length;
            for (var i = 0; i < vertexCount; ++i)
            {
                // For each name, add a new vertex and add it to the beginning of the Gremlin command list.
                var name = names[i];
                queries.Insert(0, new GremlinQuery($"Add {name}", GetVertexStatement(name)));

                // Build up to 8 outgoing edges on the current name.
                // Get some random indices of other names...
                var targetIndices = Enumerable.Range(0, vertexCount - 1).OrderBy(x => rand.Next()).Take(8).Where(x => x != i);

                // ... and add an edge towards each of these people.
                foreach (var targetIndex in targetIndices)
                {
                    var targetName = names[targetIndex];
                    queries.Add(new GremlinQuery($"{name} knows {targetName}", GetEdgeStatement(name, targetName)));
                }
            }

            // Before executing any queries, we will reset the graph by dropping it.
            queries.Insert(0, new GremlinQuery("Drop existing Graph", "g.V().drop()"));
            return queries;
        }

        private static async Task ExecuteGraphQueriesAsync(GremlinServer gremlinServer, IEnumerable<GremlinQuery> queries)
        {
            using (var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(), GremlinClient.GraphSON2MimeType))
            {
                foreach (var query in queries)
                {
                    try
                    {
                        Console.Write($"Executing: {query.Description}... ");
                        await gremlinClient.SubmitAsync<dynamic>(query.Statement);
                        Console.WriteLine("ok");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error: {ex.Message}");
                        break;
                    }
                }
            }
        }

        private static string GetVertexStatement(string name) => $"g.addV('person').property('id', '{name}')";

        private static string GetEdgeStatement(string from, string to) => $"g.V('{from}').addE('knows').to(g.V('{to}'))";
    }
}
