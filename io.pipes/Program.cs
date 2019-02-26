using System;
using System.Buffers;
using System.Buffers.Text;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Buffers.Writer;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Text.Http.Formatter;
using System.Text.Http.Parser;

namespace io.pipes
{
    class Program
    {
        private static readonly HttpClient _httpClient = new HttpClient(new HttpClientHandler()
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
        })
        {
            BaseAddress = new Uri("https://consumption.azure.com/v3/"),
            Timeout = TimeSpan.FromMinutes(10)
        };

        static async Task Main(string[] args)
        {
            var enrollmentNumber = ConfigurationManager.AppSettings["enrollment"];
            var accessToken = ConfigurationManager.AppSettings["token"];

            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
            _httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken);

            // var responseStream = await _httpClient.GetStreamAsync($"enrollments/{enrollmentNumber}/usagedetails/download?billingPeriod=201902");
            // var responseMessage = await _httpClient.GetAsync($"enrollments/{enrollmentNumber}/usagedetails/download?billingPeriod=201902");

            // var pipeHttpClient = new PipeHttpClient( );

            IPHostEntry ipHostInfo = Dns.GetHostEntry("consumption.azure.com");

            using (var clientSocketPipe =  await SocketPipe.ConnectAsync(ipHostInfo.HostName, 443, false))
            {
                var pipeHttpClient = new PipeHttpClient(clientSocketPipe);

                // var request = new GetFileRequest($"v3/enrollments/{enrollmentNumber}/usagedetails/download?billingPeriod=201902", accessToken);
                var request = new GetFileRequest($"v3/enrollments/{enrollmentNumber}/billingPeriods", accessToken);

                try
                {
                    StorageResponse response =
                        await pipeHttpClient.SendRequest<GetFileRequest, StorageResponse>(request);

                    if (response.StatusCode != 200)
                    {
                    }
                }
                catch (Exception ex)
                {
                    var msg=ex.Message;
                }
                //ulong bytesToRead = response.ContentLength;
                //using (var file = new FileStream(localFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
                //{
                //    await file.WriteAsync(response.Body, bytesToRead);
                //}
            }
        }
    }

    public interface IPipeWritable
    {
        Task WriteAsync(PipeWriter writer);
    }

    public class SocketPipe : IDisposable, IDuplexPipe
    {
        readonly Pipe _requestPipe;
        readonly Pipe _responsePipe;
        readonly Socket _socket;
        readonly Stream _stream;

        // TODO (pri 3): would be nice to make this whole struct read-only
        Task _responseReader;
        Task _requestWriter;
        public TraceSource Log;

        SocketPipe(Socket socket, SslStream stream)
        {
            _socket = socket;
            _stream = stream;
            _requestPipe = new Pipe();
            _responsePipe = new Pipe();
            _responseReader = null;
            _requestWriter = null;
            Log = null;
        }

        public static async Task<SocketPipe> ConnectAsync(string host, int port, bool tls = false)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);

            // TODO (pri 3): all this TLS code is not tested
            // TODO (pri 3): would be great to get flat APIs for TLS
            SslStream tlsStream = null;
            if (tls)
            {
                var networkStream = new NetworkStream(socket, FileAccess.Read, false);
                tlsStream = new SslStream(networkStream);
                await tlsStream.AuthenticateAsClientAsync(host).ConfigureAwait(false);
            }
            else
            {
                await socket.ConnectAsync(host, port).ConfigureAwait(false);
            }

            var client = new SocketPipe(socket, tlsStream);
            client._responseReader = client.ReceiveAsync();
            client._requestWriter = client.SendAsync();

            return client;
        }

        async Task SendAsync()
        {
            PipeReader reader = _requestPipe.Reader;
            try
            {
                while (true)
                {
                    ReadResult result = await reader.ReadAsync();
                    ReadOnlySequence<byte> buffer = result.Buffer;

                    try
                    {
                        if (!buffer.IsEmpty)
                        {
                            for (SequencePosition position = buffer.Start; buffer.TryGet(ref position, out ReadOnlyMemory<byte> segment);)
                            {
                                if (Log != null && Log.Switch.ShouldTrace(TraceEventType.Verbose))
                                {
                                    string data = Encodings.Utf8.ToString(segment.Span);
                                    if (!string.IsNullOrWhiteSpace(data))
                                        Log.TraceInformation(data);
                                }

                                await WriteToSocketAsync(segment).ConfigureAwait(false);
                            }
                        }
                        else if (result.IsCompleted)
                        {
                            break;
                        }
                    }
                    finally
                    {
                        reader.AdvanceTo(buffer.End);
                    }
                }
            }
            catch (Exception e)
            {
                Log.TraceEvent(TraceEventType.Error, 0, e.ToString());
            }
            finally
            {
                reader.Complete();
            }
        }

        async Task ReceiveAsync()
        {
            PipeWriter writer = _responsePipe.Writer;
            try
            {
                while (true)
                {
                    // just wait for data in the socket
                    await ReadFromSocketAsync(Memory<byte>.Empty);

                    while (HasData)
                    {
                        Memory<byte> buffer = writer.GetMemory();
                        int readBytes = await ReadFromSocketAsync(buffer).ConfigureAwait(false);
                        if (readBytes == 0) break;

                        if (Log != null && Log.Switch.ShouldTrace(TraceEventType.Verbose))
                        {
                            string data = Encodings.Utf8.ToString(buffer.Span.Slice(0, readBytes));
                            if (!string.IsNullOrWhiteSpace(data))
                                Log.TraceInformation(data);
                        }

                        writer.Advance(readBytes);
                        await writer.FlushAsync();
                    }
                }
            }
            finally
            {
                writer.Complete();
            }
        }

        async Task WriteToSocketAsync(ReadOnlyMemory<byte> buffer)
        {
            if (_stream != null)
            {
                await _stream.WriteAsync(buffer).ConfigureAwait(false);
                await _stream.FlushAsync().ConfigureAwait(false);
            }
            else
            {
                await _socket.SendAsync(buffer, SocketFlags.None).ConfigureAwait(false);
            }
        }

        async ValueTask<int> ReadFromSocketAsync(Memory<byte> buffer)
        {
            if (_stream != null)
            {
                return await _stream.ReadAsync(buffer).ConfigureAwait(false);
            }
            else
            {
                return await _socket.ReceiveAsync(buffer, SocketFlags.None).ConfigureAwait(false);
            }
        }

        bool HasData
        {
            get
            {
                if (_stream != null) return _stream.Length != 0;
                return _socket.Available != 0;
            }
        }

        public void Dispose()
        {
            _stream?.Dispose();
            _socket.Dispose();
        }

        public bool IsConnected => _socket != null;

        public PipeReader Input => _responsePipe.Reader;

        public PipeWriter Output => _requestPipe.Writer;
    }

    public class StorageClient : IDisposable
    {
        PipeHttpClient _client;
        SocketPipe _pipe;
        //Sha256 _hash;
        string _host;
        int _port;
        string _accountName;

        //public StorageClient(ReadOnlySpan<char> masterKey, ReadOnlySpan<char> accountName, ReadOnlySpan<char> host, int port = 80, TraceSource log = null)
        //{
        //    _host = new string(host);
        //    _accountName = new string(accountName);
        //    _port = port;
        //    byte[] keyBytes = Key.ComputeKeyBytes(masterKey);
        //    _hash = Sha256.Create(keyBytes);
        //    Log = log;
        //}

        //public StorageClient(byte[] keyBytes, ReadOnlySpan<char> accountName, ReadOnlySpan<char> host, int port = 80, TraceSource log = null)
        //{
        //    _host = new string(host);
        //    _accountName = new string(accountName);
        //    _port = port;
        //    _hash = Sha256.Create(keyBytes);
        //    Log = log;
        //}

        public TraceSource Log { get; }

        public string Host => _host;

        public string AccountName => _accountName;

        //internal Sha256 Hash => _hash;

        public async ValueTask<StorageResponse> SendRequest<TRequest>(TRequest request)
            where TRequest : IStorageRequest
        {
            if (!_client.IsConnected)
            {
                _pipe = await SocketPipe.ConnectAsync(_host, _port).ConfigureAwait(false);
                _pipe.Log = Log;
                _client = new PipeHttpClient(_pipe);
            }
            request.Client = this;

            StorageResponse response = await _client.SendRequest<TRequest, StorageResponse>(request).ConfigureAwait(false);
            if (request.ConsumeBody) await ConsumeResponseBody(response.Body);
            return response;
        }

        // for some reason some responses contain a body, despite the fact that the MSDN docs say there is no body, so
        // I need to skip the body without understanding what it is (it's "0\n\r\n\r", BTW)
        static async Task ConsumeResponseBody(PipeReader reader)
        {
            ReadResult body = await reader.ReadAsync();
            ReadOnlySequence<byte> bodyBuffer = body.Buffer;
            reader.AdvanceTo(bodyBuffer.End);
        }

        public void Dispose()
        {
            _pipe.Dispose();
            //_hash.Dispose();
        }
    }

    public struct StorageResponse : IHttpResponseHandler
    {
        static byte[] s_contentLength = Encoding.UTF8.GetBytes("Content-Length");

        ulong _contentLength;
        public ulong ContentLength => _contentLength;
        public ushort StatusCode { get; private set; }
        public PipeReader Body { get; private set; }

        public void OnStatusLine(Http.Version version, ushort statusCode, ReadOnlySpan<byte> status)
        {
            StatusCode = statusCode;
        }

        public void OnHeader(ReadOnlySpan<byte> name, ReadOnlySpan<byte> value)
        {
            if (name.SequenceEqual(s_contentLength))
            {
                if (!Utf8Parser.TryParse(value, out _contentLength, out _))
                {
                    throw new Exception("invalid header");
                }
            }
        }

        public ValueTask OnBody(PipeReader body)
        {
            Body = body;
            return default;
        }
    }

    public interface IStorageRequest : IPipeWritable
    {
        StorageClient Client { get; set; }
        string RequestPath { get; }
        string CanonicalizedResource { get; }
        string AccessToken { get; }
        long ContentLength { get; }
        bool ConsumeBody { get; }
    }

    public abstract class RequestWriter<T> where T : IPipeWritable
    {
        public abstract Http.Method Verb { get; }

        public async Task WriteAsync(PipeWriter writer, T request)
        {
            WriteRequestLineAndHeaders(writer, ref request);
            await WriteBody(writer, request).ConfigureAwait(false);
            await writer.FlushAsync();
        }

        // TODO (pri 2): writing the request line should not be abstract; writing headers should.
        protected abstract void WriteRequestLineAndHeaders(PipeWriter writer, ref T request);
        protected virtual Task WriteBody(PipeWriter writer, T request) { return Task.CompletedTask; }
    }

    abstract class StorageRequestWriter<T> : RequestWriter<T> where T : IStorageRequest
    {
        // TODO (pri 3): this should be cached with some expiration policy
        protected DateTime Time => DateTime.UtcNow;

        // TODO (pri 2): it would be good if this could advance and flush instead demanding larger and larger buffers.
        protected override void WriteRequestLineAndHeaders(PipeWriter writer, ref T arguments)
        {
            Span<byte> memory = writer.GetSpan();
            BufferWriter bufferWriter = memory.AsHttpWriter();
            bufferWriter.Enlarge = (int desiredSize) =>
            {
                return writer.GetMemory(desiredSize);
            };

            bufferWriter.WriteRequestLine(Verb, Http.Version.Http11, arguments.RequestPath);

            int headersStart = bufferWriter.WrittenCount;
            WriteXmsHeaders(ref bufferWriter, ref arguments);
            Span<byte> headersBuffer = bufferWriter.Written.Slice(headersStart);

            //var authenticationHeader = new StorageAuthorizationHeader()
            //{
            //    // TODO (pri 1): the hash is not thread safe. is that OK?
            //    Hash = arguments.Client.Hash,
            //    HttpVerb = AsString(Verb),
            //    AccountName = arguments.Client.AccountName,
            //    CanonicalizedResource = arguments.CanonicalizedResource,
            //    // TODO (pri 1): this allocation should be eliminated
            //    CanonicalizedHeaders = headersBuffer.ToArray(),
            //    ContentLength = arguments.ContentLength
            //};
            // TODO (pri 3): the default should be defaulted
            bufferWriter.WriteHeader("Authorization", $"Bearer {arguments.AccessToken}");

            WriteOtherHeaders(ref bufferWriter, ref arguments);
            bufferWriter.WriteEoh();

            writer.Advance(bufferWriter.WrittenCount);
        }

        protected abstract void WriteXmsHeaders(ref BufferWriter writer, ref T arguments);

        protected virtual void WriteOtherHeaders(ref BufferWriter writer, ref T arguments)
        {
            writer.WriteHeader("Content-Length", arguments.ContentLength);
            // writer.WriteHeader("Host", arguments.Client.Host);
        }

        static readonly byte[] s_GETu8 = Encoding.ASCII.GetBytes("GET");
        static readonly byte[] s_PUTu8 = Encoding.ASCII.GetBytes("PUT");
        public static ReadOnlyMemory<byte> AsString(Http.Method verb)
        {
            if (verb == Http.Method.Get) return s_GETu8;
            if (verb == Http.Method.Put) return s_PUTu8;
            throw new NotImplementedException();
        }
    }

    public struct GetFileRequest : IStorageRequest
    {
        public StorageClient Client { get; set; }
        public string FilePath { get; set; }
        public string AccessToken { get;  }

        public GetFileRequest(string filePath, string accessToken)
        {
            FilePath = filePath;
            Client = null;
            AccessToken = accessToken;
        }

        public long ContentLength => 0;
        public string RequestPath => FilePath;
        public string CanonicalizedResource => FilePath;
        public bool ConsumeBody => false;

        public async Task WriteAsync(PipeWriter writer)
            => await requestWriter.WriteAsync(writer, this).ConfigureAwait(false);

        static readonly Writer requestWriter = new Writer();

        class Writer : StorageRequestWriter<GetFileRequest>
        {
            public override Http.Method Verb => Http.Method.Get;

            protected override void WriteXmsHeaders(ref BufferWriter writer, ref GetFileRequest arguments)
            {
                writer.WriteHeader("x-ms-date", Time, 'R');
                writer.WriteHeader("x-ms-version", "2017-04-17");
            }
        }
    }

    public readonly struct PipeHttpClient
    {
        static readonly HttpParser s_headersParser = new HttpParser();

        readonly IDuplexPipe _pipe;

        public PipeHttpClient(IDuplexPipe pipe)
        {
            _pipe = pipe;
        }

        public async ValueTask<TResponse> SendRequest<TRequest, TResponse>(TRequest request)
            where TRequest : IPipeWritable
            where TResponse : IHttpResponseHandler, new()
        {
            await request.WriteAsync(_pipe.Output).ConfigureAwait(false);

            PipeReader reader = _pipe.Input;
            TResponse response = await ParseResponseAsync<TResponse>(reader).ConfigureAwait(false);
            await response.OnBody(reader);
            return response;
        }

        static async ValueTask<T> ParseResponseAsync<T>(PipeReader reader)
            where T : IHttpResponseHandler, new()
        {
            var handler = new T();
            while (true)
            {
                ReadResult result = await reader.ReadAsync();
                ReadOnlySequence<byte> buffer = result.Buffer;
                // TODO (pri 2): this should not be static, or ParseHeaders should be static
                if (s_headersParser.ParseHeaders(handler, buffer, out int rlConsumed))
                {
                    reader.AdvanceTo(buffer.GetPosition(rlConsumed));
                    break;
                }
                reader.AdvanceTo(buffer.Start, buffer.End);
            }

            while (true)
            {
                ReadResult result = await reader.ReadAsync();
                ReadOnlySequence<byte> buffer = result.Buffer;
                if (s_headersParser.ParseHeaders(handler, buffer, out int hdConsumed))
                {
                    reader.AdvanceTo(buffer.GetPosition(hdConsumed));
                    break;
                }
                reader.AdvanceTo(buffer.Start, buffer.End);
            }

            await handler.OnBody(reader);
            return handler;
        }

        public bool IsConnected => _pipe != null;
    }

    public interface IHttpResponseHandler : IHttpHeadersHandler, IHttpResponseLineHandler
    {
        ValueTask OnBody(PipeReader body);
    }

    static class PipelinesExtensions
    {
        /// <summary>
        /// Copies bytes from ReadOnlySequence to a Stream
        /// </summary>
        public static async Task WriteAsync(this Stream stream, ReadOnlySequence<byte> buffer)
        {
            for (SequencePosition position = buffer.Start; buffer.TryGet(ref position, out var memory);)
            {
                await stream.WriteAsync(memory).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Copies bytes from PipeReader to a Stream
        /// </summary>
        public static async Task WriteAsync(this Stream stream, PipeReader reader, ulong bytes)
        {
            while (bytes > 0)
            {
                ReadResult result = await reader.ReadAsync();
                ReadOnlySequence<byte> bodyBuffer = result.Buffer;
                if (bytes < (ulong)bodyBuffer.Length)
                {
                    throw new NotImplementedException();
                }
                bytes -= (ulong)bodyBuffer.Length;
                await stream.WriteAsync(bodyBuffer).ConfigureAwait(false);
                await stream.FlushAsync().ConfigureAwait(false);
                reader.AdvanceTo(bodyBuffer.End);
            }
        }

        /// <summary>
        /// Copies bytes from Stream to PipeWriter 
        /// </summary>
        public static async Task WriteAsync(this PipeWriter writer, Stream stream, long bytesToWrite)
        {
            if (!stream.CanRead) throw new ArgumentException("Stream.CanRead returned false", nameof(stream));
            while (bytesToWrite > 0)
            {
                Memory<byte> buffer = writer.GetMemory();
                if (buffer.Length > bytesToWrite)
                {
                    buffer = buffer.Slice(0, (int)bytesToWrite);
                }
                if (buffer.Length == 0) throw new NotSupportedException("PipeWriter.GetMemory returned an empty buffer.");
                int read = await stream.ReadAsync(buffer).ConfigureAwait(false);
                if (read == 0) return;
                writer.Advance(read);
                bytesToWrite -= read;
                await writer.FlushAsync();
            }
        }
    }
}
