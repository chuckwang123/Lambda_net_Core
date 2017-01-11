using System;
using System.IO;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using AWSLambdaLog.Model;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializerAttribute(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace AWSLambdaLog
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }
        
        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Bucket = "junyuevernote";
            var s3 = new S3Bucket
            {
                BucketName = s3Bucket
            };

            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null)
            {
                return null;
            }
            //Get the Setting for Destination
            var srcKey = s3Event.Object.Key;
            var settings = GetOutputSettings(srcKey);
            

            try
            {
                //Download the file
                var getObjectRequest = new GetObjectRequest
                {
                    BucketName = s3Bucket,
                    Key = settings.SrcKey
                };
                var getObjectResponse = await S3Client.GetObjectAsync(getObjectRequest);
                using (var reader = new StreamReader(getObjectResponse.ResponseStream))
                {
                    var contents = reader.ReadToEnd();
                }


                //Transfer it
                //var item = JsonConvert.DeserializeObject<>(context);

                //Upload it
                
                return "Success";
            }
            catch(Exception e)
            {
                context.Logger.LogLine($"Error getting object {s3Event.Bucket.Name} from bucket {s3Event.Object.Key}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        private static Setting GetOutputSettings(string sourceKey)
        {
            return new Setting
            {
                DstKey = "InputFolder/" + sourceKey,
                SrcKey = sourceKey
            };
        }
    }
}
