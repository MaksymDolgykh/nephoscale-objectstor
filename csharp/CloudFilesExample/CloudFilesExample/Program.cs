using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using Rackspace.Cloudfiles;

// csharp-cloudfiles git repo
// https://github.com/jmeridth/csharp-cloudfiles
// NuGet package
// https://www.nuget.org/packages/csharp-cloudfiles/

namespace CloudFilesExample
{
    class Program
    {
        static void Main()
        {
            // put your nephoscale storage credentials here, described in this doc
            // http://docs.nephoscale.com/#!/reference/objectstor
            var username = "NEPHOSCALE_SLICE_ID:SLICE_USERNAME";
            var password = "";
            var Url = "https://data.sjc1.nephoscale.com/auth/v1.0";

            // we are going to create this container and object
            var TestContainerName = "TestContainer";
            var TestFileName = "HelloWorld.txt";

            // CF_Connection() EXAMPLE: Make a connection
            var userCreds = new UserCredentials(username, password, Url);
            var CONNECTION = new CF_Connection(userCreds);
            CONNECTION.Authenticate();
            CONNECTION.Retries = 3;

            // CF_Account() EXAMPLE: create account object to retrieve account information, 
            // stats, list of the containers, create containers etc
            var ACCOUNT = new CF_Account(CONNECTION);
            ACCOUNT.Retries = 3;

            // CreateContainer() EXAMPLE: Create container with name {TestContainer}
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Creating container \"" +TestContainerName+"\"");
            var CONTAINER = ACCOUNT.CreateContainer(TestContainerName);
            CONTAINER.Retries = 3;

            // CreateObject() EXAMPLE: Create new file object in the new container created 
            // in the previous step
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Creating new file object \""+TestFileName+"\" in the container \""+TestContainerName+"\"");
            var FileObject = CONTAINER.CreateObject(TestFileName);

            // WriteFromFile() EXAMPLE: put content of LOCAL file {TestFileName} to the file object
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Writing local file \""+TestFileName+"\" to the object on the storage");
            //var LocalFile = "../../tests/HelloWorld.txt";
            FileObject.WriteFromFile("../../tests/"+TestFileName);

            System.Threading.Thread.Sleep(3000);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Getting list of containers...");
            // GetContainers() EXAMPLE: Get a list of all containers in the account
            var containers = ACCOUNT.GetContainers();
            foreach (var container in containers)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Container {0}", container.Name);
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Getting list of Objects in the container {0}...", container.Name);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Objects in the container {0}:", container.Name);

                // GetObjects() EXAMPLE: Get the list of file objects from the particular container
                // get container object for the particular container name, retrieve container information, 
                // list objects(files) in the particular container etc
                CONTAINER = ACCOUNT.GetContainer(container.Name);
                CONTAINER.Retries = 3;
                var FileObjects = CONTAINER.GetObjects();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Name \t\t Size \t Type \t\t Last Modified");
                // DESCRIPTION: file object.Headers is a dictionary with the following Keys
                // x-timestamp , x-trans-id , connection , accept-ranges , content-length
                // content-type , date , etag , last-modified
                foreach (var obj in FileObjects)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.Write("{0} \t {1} \t {2} \t {3}", obj.Name, obj.Headers["content-length"], obj.Headers["content-type"], obj.Headers["last-modified"]);
                    Console.WriteLine();
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("Reading content of the storage object \"{0}\" and printing it out", obj.Name);
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.BackgroundColor = ConsoleColor.Gray;
                    // SaveToFile() EXAMPLE:  Read file object from the storage and save it to the LOCAL file
                    obj.SaveToFile("../../tests/tmp_" + obj.Name);
                    // Print out file content
                    StreamReader file = new StreamReader("../../tests/tmp_" + obj.Name);
                    string line;
                    while ((line = file.ReadLine()) != null)
                    {
                        Console.WriteLine(line);
                    }
                    file.Close();
                    // Delete temp LOCAL file
                    System.IO.File.Delete("../../tests/tmp_" + obj.Name);
                }

            }

            // Redo
            Console.BackgroundColor = ConsoleColor.Black;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Press ENTER to remove all objects and containers we created in the previous steps.");
            Console.ReadLine();

            // GetContainer() EXAMPLE: create container object by container name
            CONTAINER = ACCOUNT.GetContainer(TestContainerName);
            CONTAINER.Retries = 3;

            // Delete all object from the container {TestContainerName}
            // GetObjects() EXAMPLE:
            var Files = CONTAINER.GetObjects();
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Deleting all objects from the container "+TestContainerName);
            foreach (var obj in Files)
            {
                Console.WriteLine("Deleting object " + obj.Name + " from container " + TestContainerName);
                // DeleteObject() EXAMPLE: Delete file from Object Storage
                CONTAINER.DeleteObject(obj.Name);
            }


            // DeleteContainer() EXAMPLE: Delete container
            Console.WriteLine("Deleting container "+ TestContainerName);
            // According to CAP theorem
            // https://en.wikipedia.org/wiki/CAP_theorem
            // swift storage is eventually consistent
            // https://en.wikipedia.org/wiki/Eventual_consistency
            /// So let's wait a few seconds and give a few tries so  object storage 
            /// can complete all DeleteObject() requests
            // otherwise it won't allow to container (not empty)
            while (true)
            {
                System.Threading.Thread.Sleep(5000);
                try
                {
                    ACCOUNT.DeleteContainer(TestContainerName);
                    break;
                }
                catch (Exception e)
                {
                    continue;
                }
            }
                
            
            Console.WriteLine("Press ENTER to finish progaram and close console window");
            Console.ReadLine();
        }
    }
}
