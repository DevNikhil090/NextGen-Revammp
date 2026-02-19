using APIService.BusinessLogic.Interfaces.Administration;
using APIService.DataAccess.Entities.Models.Report;
using APIService.DataAccess.Enums;
using APIService.DataAccess.Interfaces.Administration;
using APIService.DataAccess.Interfaces.Base;
using APIService.DataAccess.Repositories.Base;
using APIService.DTOs;
using APIService.DTOs.Administration.User;
using APIService.EmailService.Interfaces;
using APIService.ReportService.XrmDotNetReport.Dtos;
using APIService.ReportService.XrmDotNetReport.Dtos.SaveReport;
using APIService.ReportService.XrmDotNetReport.Enums;
using APIService.ReportService.XrmDotNetReport.Interfaces;
using APIService.ReportService.XrmDotNetReport.Models;
using APIService.ReportService.XrmDotNetReport.Repository;
using AutoMapper;
using CachingService.Services;
using CombineAutoProcess.DataAccess.Interface.Common;
using CommonUtilities;
using CommonUtilities.DTOs;
using CommonUtilities.Enums;
using CommonUtilities.ResponseTypes;
using CommonUtilities.Services;
using CommonUtilities.Validations;
using DMSService.BusinessLogic.Interfaces;
using EmailService.DTOs;
using EmailService.Interfaces;
using FluentValidation;
using FTPService.Dto;
using FTPService.Interfaces;
using LocalizationService.Interfaces;
using Microsoft.EntityFrameworkCore;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Web;
using XrmMQLibrary.Enums;

namespace APIService.ReportService.XrmDotNetReport.Service
{
    /// <summary>
    /// Provides functionality to run reports and handle related operations within the system.
    /// </summary>
    public partial class XrmRunReportService : AuxiliaryRepository, IXrmRunReportService
    {
        private readonly IXrmReportApiRepository _repository;
        private readonly IValidator<RunReportApiCallDto> _validator;
        private readonly IXrmRunReportRepository _xrmRunReportRepository;
        private readonly IServiceProvider _serviceProvider;
        private readonly IXrmReportCommonRepository _xrmReportCommonRepository;
        private readonly IEmailSendService _emailSendService;
        private readonly IDataPaginationRepository _paginationRepository;
        private readonly int _loggedInUserNo;
        private readonly IUserDetailRepository _userDetailRepository;
        private readonly int _loggedInUserGroupId;
        private readonly IFileManagerService _fileManagerService;
        private readonly IEmailTemplateConfiguration _emailTemplateConfiguration;
        private readonly IReviewLinkGenerator _reviewLinkGenerator;
        private readonly IFtpUploader _ftpUploader;
        private const int ActionId = (int)ActionEnum.SendToSftpNexteer;
        private readonly ICommonRepository _commonRepository;
        private readonly DateTime _getCurrentDateTime;
        private readonly ICacheService _cacheService;
        private readonly ICacheKeyService _cacheKeyService;
        private readonly ITimeZoneConfigService _timeZoneConfigService;
        private readonly IEmailSaveLogRepo _emailSaveLogRepo;

        /// <summary>
        /// Initializes a new instance of the <see cref="XrmRunReportService"/> class.
        /// </summary>
        /// <param name="repository">The repository for interacting with the XRM Report API.</param>        
        /// <param name="validator">The validator for validating <see cref="RunReportApiCallDto"/> objects.</param>
        /// <param name="mapper">The object mapper for mapping between DTOs and domain objects.</param>
        /// <param name="xrmRunReportRepository">The repository for handling report run-related data.</param>
        /// <param name="serviceProvider">An instance of <see cref="IServiceProvider"/> used to resolve instances.</param> 
        ///<param name="xrmReportCommonRepository">An instance of <see cref="IXrmReportCommonRepository"/>for hub details</param>
        ///<param name="emailSendService">An instance of <see cref="IEmailSendService"/>for email details.</param>
        ///<param name="dataPagination">An instance of <see cref="IDataPaginationRepository"/></param>
        ///<param name="userDetailRepository"> An instance of <see cref="IUserDetailService"/>></param>
        ///<param name="fileManagerService"> An instance of <see cref="IFileManagerService"/> for file management operations.</param>
        ///<param name="emailTemplateConfiguration">An instance of <see cref="IEmailTemplateConfiguration"/> for email template configurations.</param>
        ///<param name="reviewLinkGenerator">An instance of <see cref="IReviewLinkGenerator"/> for generating review links.</param>        
        ///<param name="ftpUploader">An instance of <see cref="IFtpUploader"/> for FTP operations.</param>
        ///<param name="commonRepository">An instance of <see cref="ICommonRepository"/> for common data access operations.</param>
        ///<param name="cacheKeyService">An instance of <see cref="ICacheKeyService"/> for cache key management.</param>
        ///<param name="cacheService">An instance of <see cref="ICacheService"/> for caching operations.</param>
        ///<param name="timeZoneConfigService">An instance of <see cref="ITimeZoneConfigService"/> for time zone configurations.</param>
        /// <param name="emailSaveLogRepo"></param>
        public XrmRunReportService(IXrmReportApiRepository repository,
            IValidator<RunReportApiCallDto> validator, IMapper mapper, IXrmRunReportRepository xrmRunReportRepository,
            IServiceProvider serviceProvider, IXrmReportCommonRepository xrmReportCommonRepository,
            IEmailSendService emailSendService, IDataPaginationRepository dataPagination, IUserDetailRepository userDetailRepository,
            IFileManagerService fileManagerService, IEmailTemplateConfiguration emailTemplateConfiguration,
            IReviewLinkGenerator reviewLinkGenerator, IFtpUploader ftpUploader, ICommonRepository commonRepository,
            ICacheService cacheService, ICacheKeyService cacheKeyService,
            ITimeZoneConfigService timeZoneConfigService, IEmailSaveLogRepo emailSaveLogRepo) : base(mapper)
        {
            _repository = repository;
            _validator = validator;
            _xrmRunReportRepository = xrmRunReportRepository;
            _serviceProvider = serviceProvider;
            _xrmReportCommonRepository = xrmReportCommonRepository;
            _emailSendService = emailSendService;
            _paginationRepository = dataPagination;
            _loggedInUserNo = CommonUtilities.UserInfo.GetLoggedInUserNo();
            _loggedInUserGroupId = CommonUtilities.UserInfo.GetLoggedInRoleGroupId();
            _userDetailRepository = userDetailRepository;
            _fileManagerService = fileManagerService;
            _emailTemplateConfiguration = emailTemplateConfiguration;
            _reviewLinkGenerator = reviewLinkGenerator;
            _ftpUploader = ftpUploader;
            _commonRepository = commonRepository;
            _getCurrentDateTime = DateService.GetCurrentDateTime;
            _cacheService = cacheService;
            _cacheKeyService = cacheKeyService;
            _timeZoneConfigService = timeZoneConfigService;
            _emailSaveLogRepo = emailSaveLogRepo;
        }

        private static string PreprocessJsonDateFieldsAsync(string json, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Regex.Replace(json, @"\\/Date\((\d+)\)\\/", match =>
                {
                    var milliseconds = long.Parse(match.Groups[1].Value);
                    var dateTime = DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).UtcDateTime;
                    return dateTime.ToString("yyyy-MM-dd");
                }, RegexOptions.None, TimeSpan.FromMilliseconds(100));
            }
            catch (OperationCanceledException)
            {
                return ReportConstant.ReportTimeOutMessage;
            }
        }
        /// <summary>
        /// Processes the JSON report data based on the provided <see cref="RunReportApiCallDto"/> and settings.
        /// </summary>
        /// <param name="callDto">The DTO containing report details and filters to be processed.</param>
        /// <param name="xrmSettings">The settings required for interacting with the XRM Report API.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        /// A task representing the asynchronous operation. The task result is a <see cref="ResponseBase"/> 
        /// indicating the success or failure of the report processing.
        /// </returns>
        private async ValueTask<ResponseBase> ProcessReportJson(RunReportApiCallDto callDto, XrmDotNetReportSettings xrmSettings, CancellationToken cancellationToken = default)
        {
            try
            {
                if (!string.IsNullOrEmpty(callDto.UKey))
                {
                    ReportHandler result;
                    var reportId = await _repository.GetReportId(callDto.UKey, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    if (reportId == null)
                        return new XrmNotFoundResponse();

                    var requestModel = JsonSerializer.Serialize(new { reportId = reportId, adminMode = true, buildSql = false });

                    var stringContentResult = await XrmCallReportApi.ExecuteCallReportApi(ReportConstant.UrlLoadReport, requestModel, xrmSettings, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    if (stringContentResult == "\"\"") { return new XrmBadRequestResponse(); }

                    var processed = PreprocessJsonDateFieldsAsync(stringContentResult, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    result = JsonSerializer.Deserialize<ReportHandler>(processed,
                        new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                    callDto.Json = result;
                    //Sorting Apply on Grid
                    var sorting = callDto.SelectedSorts ?? new List<SelectedSortDto>();
                    if (sorting.Count > 0)
                    {
                        callDto.Json.SortBy = sorting[0].FieldId;
                        callDto.Json.SortDesc = sorting[0].Descending;
                        callDto.Json.SelectedSorts = sorting.Skip(1).ToList();
                    }
                    else
                    {
                        callDto.Json.SelectedSorts = result.SelectedSorts;
                        callDto.Json.SortBy = result.SortBy;
                        callDto.Json.SortDesc = result.SortDesc;
                    }

                    if (callDto.IsDefaultFilter)
                    {
                        callDto.Json.Filters = result.Filters;
                        callDto.Json.SelectedParameters = result.SelectedParameters;
                    }
                    else
                    {
                        callDto.Json.Filters = callDto.SelectedFilters;
                        callDto.Json.SelectedParameters = callDto.SelectedParameters;
                    }

                    callDto.Json.DrillDownRow = callDto.DrillDownRow;
                    if (!string.IsNullOrEmpty(callDto.ReportType))
                    {
                        callDto.Json.ReportType = callDto.ReportType;
                    }
                }
                return new XrmSuccessResponse();
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        /// <summary>
        /// Generates an SQL ORDER BY clause based on the specified selected sorts and report settings.
        /// </summary>
        /// <param name="selectedSorts">A list of <see cref="SelectedSortDto"/> objects specifying the sort criteria.</param>        
        /// <param name="selectedFieldIDs">A list of field IDs that are selected for sorting.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        /// A <see cref="ValueTask{TResult}"/> containing a string representing the ORDER BY clause for SQL queries.
        /// If no valid sort criteria are provided, an empty string is returned.
        /// </returns>
        /// <remarks>
        /// This method constructs the ORDER BY clause by fetching field details using the API and evaluating whether
        /// the field is associated with a foreign table and value. The resulting clause supports ascending (ASC)
        /// and descending (DESC) sorting directions.
        /// </remarks>
        public async ValueTask<string> GetOrderByAsync(List<SelectedSortDto> selectedSorts, List<int?> selectedFieldIDs, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (selectedSorts == null || selectedSorts.Count == 0)
                    return string.Empty;

                var orderByParts = selectedSorts.Select(sort =>
                {
                    var index = selectedFieldIDs.IndexOf(sort.FieldId) + 1; // SQL indices start at 1
                    var direction = sort.Descending ? "DESC" : "ASC";
                    return $"{index} {direction}";
                });
                cancellationToken.ThrowIfCancellationRequested();
                var orderByClause = $"ORDER BY {string.Join(", ", orderByParts)}";
                await Task.CompletedTask;
                return orderByClause;
            }
            catch (OperationCanceledException)
            {
                return ReportConstant.ReportTimeOutMessage;
            }
        }


        ///<inheritdoc/>
        public async ValueTask<ResponseBase> RunReportAsync(RunReportApiCallDto callDto, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                //Remove objects from DrillDownRow where isPivotField is true
                if (callDto.DrillDownRow != null && callDto.DrillDownRow.Any())
                {
                    callDto.DrillDownRow = callDto.DrillDownRow
                        .Where(item => !(item?.Column.IsPivotField ?? false))
                        .ToList();
                }

                if (string.IsNullOrEmpty(callDto.UKey) && callDto.Json.DrillDownRow != null && callDto.Json.DrillDownRow.Any())
                {
                    callDto.Json.DrillDownRow = callDto.Json.DrillDownRow
                        .Where(item => !(item?.Column.IsPivotField ?? false))
                        .ToList();
                }

                var outPutType = callDto.OutputTypeId;
                ResponseBase result;

                ExecutionHistoryRequestDto request = new ExecutionHistoryRequestDto
                {
                    UKey = callDto.UKey,
                    OutputTypeId = outPutType,
                    Sql = null,
                    FilterJson = callDto.SelectedFilters,
                    SelectedParameters = callDto.SelectedParameters,
                    IsUiExport = callDto.IsUiExport,
                    CallDto = callDto,
                    SelectedSorts = callDto.SelectedSorts
                };

                switch (outPutType)
                {
                    case (int)OutputType.List or (int)OutputType.Summary or (int)OutputType.Line or (int)OutputType.Pie or (int)OutputType.Bar or (int)OutputType.Combo:
                        try
                        {

                            var output = await ReportExecutorAsync(callDto, false, cancellationToken);
                            cancellationToken.ThrowIfCancellationRequested();
                            return output;
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }

                    case (int)OutputType.Excel or (int)OutputType.Csv or (int)OutputType.Pdf or (int)OutputType.ExcelExpanded or (int)OutputType.Text:
                        result = await SubmitExecutionHistoryAsync(request, callDto, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();
                        break;

                    default:
                        result = new XrmSuccessResponse();
                        break;
                }

                if (result is XrmSuccessWithDataResponse { Data: XrmDotNetReportResultModel xrmDotNetReportResultModel })
                {
                    xrmDotNetReportResultModel.ReportSql = null;
                }
                else if (result is XrmSuccessWithDataResponse { Data: ReportExecutionHistory reportExecution })
                {
                    reportExecution.RunSql = null;
                }
                return new XrmSuccessWithDataResponse(data: result);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }


        /// <inheritdoc/>
        public async ValueTask<ResponseBase> InsertAutoScheduleAsync(RunReportApiCallDto callDto)
        {
            var outPutType = callDto.OutputTypeId;
            ExecutionHistoryRequestDto request = new ExecutionHistoryRequestDto
            {
                UKey = callDto.UKey,
                OutputTypeId = outPutType,
                Sql = null,
                FilterJson = callDto.SelectedFilters,
                SelectedParameters = callDto.SelectedParameters,
                IsUiExport = callDto.IsUiExport,
                CallDto = callDto,
                SelectedSorts = callDto.SelectedSorts
            };
            var baseurl = await _dbContext.RouteDetails.Where(r => r.Id == 1690).Select(r => r.DownstreamScheme + "://" + r.DownstreamHost + "/" + r.Environment).FirstOrDefaultAsync();
            // var baseurl = XrmReportSettings.GetSettings().ApplicationApiUrl;

            var result = await _xrmRunReportRepository.SubmitExecutionHistoryAsync(request, default, true);

            string[] apiPath = {
                       ReportConstant.AuthAPIPath,
                       ReportConstant.RunBgScheduledReportAPIPath
                };
            CommonProducer.ScheduledReportProducerAsync(result.UKey, MQReportType.AutoScheduledReport, baseurl, apiPath);

            result.FilePath = null;
            result.JsonPayload = null;
            return new XrmSuccessWithDataResponse(data: result);

        }

        /// <inheritdoc/>
        public async ValueTask<ResponseBase> RunBgScheduledReportAsync(Guid runReportUkey, CancellationToken cancellationToken = default)
        {
            try
            {
                var transactionDetails = await _dbContext.ReportExecutionHistories.FirstOrDefaultAsync(t => t.UKey == runReportUkey);
                var reportUkey = (from r in _dbContext.ReportExecutionHistories join rep in _dbContext.Reports.AsNoTracking() on r.ReportId equals rep.Id where r.UKey == runReportUkey select rep.UKey).FirstOrDefault();
                cancellationToken.ThrowIfCancellationRequested();
                var updateDto = _mapper.Map<ReportExecutionUpdateDto>(transactionDetails);

                await _xrmRunReportRepository.ProxyAuthenticateAsync(transactionDetails.ExecutedBy);
                cancellationToken.ThrowIfCancellationRequested();
                var reportRunUkey = reportUkey.ToString();

                if (transactionDetails.ReportId == null)
                {
                    reportRunUkey = string.Empty;
                }

                string jsonString = Encoding.UTF8.GetString(transactionDetails.JsonPayload);
                RunReportApiCallDto callDto = JsonSerializer.Deserialize<RunReportApiCallDto>(jsonString);

                await RunReportFileAsync(transactionDetails.Id, callDto.OutputTypeId, reportRunUkey, updateDto, transactionDetails.FilterJson,
                    callDto.SelectedParameters, callDto.IsUiExport, callDto, cancellationToken, transactionDetails.SortingJson);

                return new XrmSuccessWithDataResponse();
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }
        /// <summary>
        /// Executes the report and handles execution history for list or summary output types.
        /// </summary>
        /// <param name="callDto">The data transfer object containing the report execution details.</param>
        /// <param name="isChart">Indicates whether the report is a chart.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>A response containing the report data if successful, or a success response otherwise.</returns>
        private async ValueTask<ResponseBase> ReportExecutorAsync(RunReportApiCallDto callDto, bool isChart = false, CancellationToken cancellationToken = default)
        {
            try
            {
                var startTime = DateService.GetCurrentDateTime.TimeOfDay;

                var result = await RunReportApiAsync(callDto, isChart, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                XrmDotNetReportResultModel xrmDotNetReportResultModel = null;

                // Check if result is neither an internal server error response nor a valid success response with data
                if (result is not XrmInternalServerErrorResponse xrmInternalServer &&
                    (result is not XrmSuccessWithDataResponse successResponse ||
                     successResponse.Data is not XrmDotNetReportResultModel model))
                {
                    return result;
                }

                // Assign the model if the result is a success response
                if (result is XrmSuccessWithDataResponse successResponseData)
                {
                    xrmDotNetReportResultModel = successResponseData.Data as XrmDotNetReportResultModel;
                }

                var endTime = DateService.GetCurrentDateTime.TimeOfDay;

                string sql = null;
                string errorMessage = result.Message;
                if (!result.Succeeded && !string.IsNullOrEmpty(result.Message))
                {
                    var messageParts = result.Message.Split('$', 2);
                    if (messageParts.Length > 1)
                    {
                        sql = messageParts[1].Trim(); // Extract SQL part if present
                        if (sql.Contains("$"))
                        {
                            sql = sql.Split('$')[0].Trim(); // Remove duplicate SQL if present
                        }

                        errorMessage = messageParts[0].Trim(); // Extract original error message
                    }
                }

                var historyDto = new ExecutionHistoryDto
                {
                    UKey = callDto.UKey,
                    Sql = xrmDotNetReportResultModel?.ReportSql ?? sql,
                    OutPutTypeId = callDto.OutputTypeId,
                    FilterJson = callDto.SelectedFilters,
                    Result = result,
                    StartTime = startTime,
                    EndTime = endTime,
                    SelectedParameters = callDto.SelectedParameters,
                    IsUiExport = callDto.IsUiExport,
                    IsScheduledReport = callDto.IsScheduledReport
                };

                await HandleExecutionHistoryAsync(historyDto, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                if (xrmDotNetReportResultModel != null)
                {
                    xrmDotNetReportResultModel.ReportSql = null;
                    return new XrmSuccessWithDataResponse(data: xrmDotNetReportResultModel);
                }

                if (!string.IsNullOrEmpty(result.Message))
                {
                    result.Message = result.Message.Contains(ReportConstant.TheMultiPartIdentifierError)
                        ? ReportConstant.TheMultiPartIdentifierError
                        : ReportConstant.CommonErrorMessage;
                }

                return result;
            }
            catch (OperationCanceledException)
            {
                throw;

            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(message: ReportConstant.CommonErrorMessage, ex: ex);
            }
        }

        private async Task<int> GetExecutedByAsync(RunReportApiCallDto runReportApi)
        {
            if (runReportApi.IsScheduledReport)
            {
                return await _xrmReportCommonRepository.GetOwnerIdAsync(runReportApi.UKey);
            }
            if (_loggedInUserNo > 0)
            {
                return _loggedInUserNo;
            }

            return await _xrmReportCommonRepository.GetExecutionByAsync(runReportApi.UKey);
        }

        private async ValueTask<ResponseBase> GenerateChartResponse(XrmSuccessWithDataResponse successResponse, RunReportApiCallDto runReportApi, string dateFormat)
        {
            if (successResponse.Data is not XrmDotNetReportResultModel xrmDotNetReportResultModel)
                return successResponse;

            var reportData = xrmDotNetReportResultModel.ReportData;

            if (reportData?.Columns == null || reportData.Rows == null)
                return new XrmBadRequestResponse("InvalidReportData");

            var xColumn = reportData.Columns.FirstOrDefault();
            if (xColumn == null)
                return new XrmBadRequestResponse(ReportConstant.NoColumnsFoundInReportData);

            var applicableGroupFunctions = new List<string> { "Group by Month", "Group by Year", "Group by Month/Year" };
            var timeFormat = "hh:mm tt";
            var xData = new ChartAxisDataDto
            {
                Key = xColumn.ColumnName,
                Values = reportData.Rows
         .Select(row =>
         {
             var value = row.Items.FirstOrDefault()?.FormattedValue;
             if (string.IsNullOrWhiteSpace(value))
                 return value;

             // Handle known grouping functions first
             if (applicableGroupFunctions.Contains(xColumn.AggregateFunc))
                 return value;

             // Handle month/day format (like "8/12")
             if (Regex.IsMatch(value, @"^\d+/\d+$"))
                 return value;

             // Check if the value is numeric before attempting date parsing
             if (decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out _))
                 return value;

             // ✅ Proper time-only formatting
             if (xColumn.FormatType == "Time" && TimeSpan.TryParse(value, out var timeSpan))
                 return DateTime.Today.Add(timeSpan).ToString(timeFormat);

             // ✅ Proper "Date and Time" handling
             if (xColumn.FormatType == "Date and Time" && DateTime.TryParse(value, out var dateTimeVal))
                 return dateTimeVal.ToString($"{dateFormat} {timeFormat}");

             // ✅ Proper "Date" only formatting (even if time part exists)
             if (xColumn.FormatType == "Date" && !char.IsLetter(value.Trim()[0]) && DateTime.TryParse(value, out var dateOnlyVal))
                 return dateOnlyVal.ToString(dateFormat);

             return value;
         })
         .Where(value => !string.IsNullOrEmpty(value))
         .ToList(),
                DataType = xColumn.DataType,
                FormatType = xColumn.FormatType
            };
            var pivotFieldSettings =
                reportData.Columns.Any(c => c.IsPivotField)
                    ? string.IsNullOrEmpty(runReportApi.UKey)
                        ? runReportApi.Json.SelectedFields
                            .Where(s => s.GroupInGraph == false)
                            .Select(s => s.FieldSettings)
                            .LastOrDefault()
                        : runReportApi.Json.GroupFunctionList
                            .Where(s => s.GroupInGraph == false && s.GroupFunc != "Pivot" && s.GroupFunc != "Group" && s.GroupFunc != "Group in Detail" && s.GroupFunc != "Only in Detail")
                            .Select(s => s.FieldSettings)
                            .LastOrDefault()
                    : null;

            var yData = reportData.Columns
                .Skip(1)
                .Take(5)
                .Select(yColumn => new ChartAxisDataDto
                {
                    Key = yColumn.ColumnName,
                    Values = reportData.Rows
                        .Select(row => row.Items
                            .FirstOrDefault(item => item.Column.ColumnName == yColumn.ColumnName)?.FormattedValue ?? "0")
                        .ToList(),
                    DataType = yColumn.DataType,
                    FormatType = yColumn.FormatType,
                    SeriesType = runReportApi.OutputTypeId == (int)OutputType.Combo
                            ? JsonDocument.Parse(
                                (
                                    yColumn.IsPivotField
                                ? pivotFieldSettings
                                        : runReportApi.Json.GroupFunctionList
                                            .FirstOrDefault(sf =>
                        (sf.FieldLabel ?? sf.CustomLabel) == yColumn.ColumnName)
                                            ?.FieldSettings
                                ) ?? "{}"
                              )
                              .RootElement
                      .TryGetProperty("seriesType", out var seriesType)
                          ? seriesType.GetString()
                                : null
                            : null

                })
                .ToList();

            var chartData = new ChartDataDto
            {
                ChartTypeId = runReportApi.OutputTypeId,
                ChartName = xrmDotNetReportResultModel.ReportName,
                XData = xData,
                YData = yData,
                OutPutTypeId = reportData.OutputTypeId,
                ReportSettings = runReportApi.Json.ReportSettings
            };
            var userDashBoard = await _dbContext.DashboardUserCharts.AsNoTracking()
                .FirstOrDefaultAsync(x => x.ReportUKey == runReportApi.UKey && x.UserNo == _loggedInUserNo);

            if (userDashBoard != null)
            {
                chartData.ChartTypeId = userDashBoard.ReportTypeId;
                chartData.ReportSettings = userDashBoard.ReportSettings;

                if (runReportApi.OutputTypeId == (int)OutputType.Combo)
                {
                    using var json = JsonDocument.Parse(userDashBoard.ReportSettings);
                    var root = json.RootElement;

                    var dict = new Dictionary<string, string>();

                    if (root.TryGetProperty("fieldSetting", out var fieldSettingElement) &&
                        fieldSettingElement.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var innerArray in fieldSettingElement.EnumerateArray())
                        {
                            if (innerArray.ValueKind == JsonValueKind.Array && innerArray.GetArrayLength() == 2)
                            {
                                dict[innerArray[0].GetString()] = innerArray[1].GetString();
                            }
                        }
                    }
                    foreach (var yd in chartData.YData)
                    {
                        if (dict.TryGetValue(yd.Key, out var newSeriesType))
                        {
                            yd.SeriesType = newSeriesType;
                        }
                    }
                }
            }

            return new XrmSuccessWithDataResponse(data: chartData);
        }


        ///<inheritdoc/>
        private async ValueTask<ResponseBase> RunDashboardAsync(RunReportApiCallDto runReportApi)
        {
            try
            {
                int executedBy = await GetExecutedByAsync(runReportApi);
                string dateFormat = UserInfo.GetDateFormat();

                bool isChart = true;
                var result = await RunReportApiAsync(runReportApi, isChart);
                if (result is XrmSuccessWithDataResponse successResponse)
                {
                    return await GenerateChartResponse(successResponse, runReportApi, dateFormat);
                }
                return result;
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }

        ///<inheritdoc/>
        public async ValueTask<ResponseBase> RunChartAsync(RunReportApiCallDto runReportApi)
        {
            try
            {
                int executedBy = await GetExecutedByAsync(runReportApi);
                string dateFormat = UserInfo.GetDateFormat();
                bool isChart = true;
                var result = await ReportExecutorAsync(runReportApi, isChart);

                if (result is XrmSuccessWithDataResponse successResponse)
                {
                    return await GenerateChartResponse(successResponse, runReportApi, dateFormat);
                }

                return result;
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }


        ///<inheritdoc/>
        public async ValueTask<ResponseBase> RunReportApiAsync(RunReportApiCallDto runReportApi, bool isChart, CancellationToken cancellationToken = default)
        {
            try
            {
                var validationResult = ValidationHandler<RunReportApiCallDto>.ValidateEntity(_validator, runReportApi, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                if (!validationResult.Succeeded)
                {
                    return validationResult;
                }

                var options = new JsonSerializerOptions
                {
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    WriteIndented = true
                };

                var apiSettings = XrmReportSettings.GetSettings(cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();


                await ProcessReportJson(runReportApi, apiSettings, cancellationToken);

                cancellationToken.ThrowIfCancellationRequested();
                runReportApi.Json.FolderId = await _repository.GetFolderIdAsync(runReportApi.Json.FolderId, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();

                if (runReportApi.Json.GroupFunctionList.Any(x => string.Equals(x.GroupFunc, "Group in Detail", StringComparison.OrdinalIgnoreCase))
                    && runReportApi.Json.DrillDownRow.Any())
                {
                    runReportApi.Json.IsAggregateReport = true;
                    runReportApi.Json.OnlyTop = null;
                }
                else if (runReportApi.Json.DrillDownRow?.Any() == true)
                {
                    runReportApi.Json.IsAggregateReport = false;
                    runReportApi.Json.OnlyTop = null;
                }
                if (!string.IsNullOrEmpty(runReportApi.UKey))
                {
                    for (int i = 0; i < runReportApi.Json.SelectedFields.Count; i++)
                    {
                        runReportApi.Json.GroupFunctionList[i].FieldSettings = runReportApi.Json.SelectedFields[i].FieldSettings;
                        runReportApi.Json.GroupFunctionList[i].FieldId = runReportApi.Json.SelectedFields[i].FieldId;
                        runReportApi.Json.SelectedFields[i].DataFormat = runReportApi.Json.SelectedFields[i].FieldFormat;
                        runReportApi.Json.SelectedFields[i].SelectedFieldAggregate = runReportApi.Json.SelectedFields[i].AggregateFunction;

                        if (runReportApi.Json.GroupFunctionList[i].CustomFieldDetails.Any())
                        {
                            runReportApi.Json.GroupFunctionList[i].IsCustom = true;
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < runReportApi.Json.SelectedFields.Count; i++)
                    {
                        runReportApi.Json.SelectedFields[i].AggregateFunction = runReportApi.Json.SelectedFields[i].SelectedFieldAggregate;
                    }
                }

                string reportJson;
                reportJson = JsonSerializer.Serialize(runReportApi.Json, options);

                var requestModel = JsonSerializer.Serialize(new
                {
                    adminMode = true,
                    runReportApi.SaveReport,
                    ReportJson = reportJson,
                    runReportApi.SubTotalMode,
                    userIdForFilter = string.Empty,
                }, options);

                apiSettings.UserId = string.Empty;

                apiSettings.UserRoles = new List<string>();

                var apiUrl = !runReportApi.Json.UseStoredProc && runReportApi.Json.DrillDownRow != null && runReportApi.Json.DrillDownRow.Any()
                    ? "/ReportApi/RunDrillDownReport" : ReportConstant.UrlRunReport;

                var stringCreateResult = await XrmCallReportApi.ExecuteCallReportApi(apiUrl, requestModel, apiSettings, cancellationToken);

                cancellationToken.ThrowIfCancellationRequested();
                if (stringCreateResult == "\"\"") { return new XrmBadRequestResponse(); }

                var resultSet = JsonSerializer.Deserialize<RunReportParameter>(stringCreateResult, options);

                resultSet.VisualizationType = runReportApi.Json.ReportType;
                resultSet.PageSize = runReportApi.PaginationDto.PageSize;
                resultSet.PageNumber = runReportApi.PaginationDto.StartIndex;
                resultSet.SubTotalMode = runReportApi.SubTotalMode;
                resultSet.ReportData = reportJson;
                resultSet.PivotColumn = PreparePivotData(runReportApi.Json.GroupFunctionList, cancellationToken).PivotColumn;
                resultSet.PivotFunction = PreparePivotData(runReportApi.Json.GroupFunctionList, cancellationToken).PivotFunction;
                cancellationToken.ThrowIfCancellationRequested();
                string orderByClause = string.Empty;

                List<int?> matchGroup;

                if (runReportApi.DrillDownRow?.Any() == true || runReportApi.Json.DrillDownRow?.Any() == true)
                {
                    matchGroup = runReportApi.Json.GroupFunctionList
                        .Where(x => !x.Disabled && !x.HideInDetail)
                        .Select(x => x.FieldId)
                        .ToList();
                }
                else
                {
                    if (!string.IsNullOrEmpty(resultSet.PivotColumn))
                    {
                        var pivotIndex = runReportApi.Json.GroupFunctionList
                                                          .FindIndex(x => x.GroupFunc == "Pivot");

                        // Get the first enabled column after pivot (skipping all disabled ones)
                        var nextIndex = runReportApi.Json.GroupFunctionList
                                                         .Skip(pivotIndex + 1)
                                                         .Select((x, i) => new { Item = x, Index = pivotIndex + 1 + i })
                                                         .FirstOrDefault(x => !x.Item.Disabled)?.Index;

                        matchGroup = runReportApi.Json.GroupFunctionList
                                                      .Where((x, i) =>
                                                          !x.Disabled &&
                                                          x.GroupFunc != "Only in Detail" &&
                                                          x.GroupFunc != "Group in Detail" &&
                                                          i != pivotIndex &&
                                                          (nextIndex == null || i != nextIndex) // exclude nextIndex only if found
                                                      )
                                                      .Select(x => x.FieldId)
                                                      .ToList();
                    }
                    else
                    {
                        // Normal logic: Exclude 'Only in Detail', 'Group in Detail', Disabled fields
                        matchGroup = runReportApi.Json.GroupFunctionList
                            .Where(x => !x.Disabled && x.GroupFunc != "Only in Detail" && x.GroupFunc != "Group in Detail" && x.GroupFunc != "Pivot")
                            .Select(x => x.FieldId)
                            .ToList();
                    }
                }

                var selectedFieldIds = runReportApi.Json.SelectedFieldIDs
                                                        .Where(id => matchGroup.Contains(id))
                                                        .ToList();

                var sortByDefault = new List<SelectedSortDto>();
                if (!runReportApi.Json.UseStoredProc)
                {
                    if (matchGroup.Contains(runReportApi.Json.SortBy))
                    {
                        sortByDefault.Add(new SelectedSortDto
                        {
                            FieldId = runReportApi.Json.SortBy,
                            Descending = runReportApi.Json.SortDesc
                        });
                    }

                    sortByDefault.AddRange(runReportApi.Json.SelectedSorts
                        .Where(s => matchGroup.Contains(s.FieldId)));

                    orderByClause = await GetOrderByAsync(sortByDefault, selectedFieldIds, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                }

                var selectedSorts = new List<SelectedSortDto>();

                if (!runReportApi.Json.UseStoredProc)
                {
                    selectedSorts.Add(new SelectedSortDto
                    {
                        FieldId = runReportApi.Json.SortBy,
                        Descending = runReportApi.Json.SortDesc
                    });

                    selectedSorts.AddRange(runReportApi.Json.SelectedSorts);
                }

                List<SelectedParameterDto> selectedParameters = new();

                selectedParameters = runReportApi.Json.DrillDownRow != null && runReportApi.Json.DrillDownRow.Any()
                                        ? runReportApi.Json.SelectedParameters
                                        : runReportApi.SelectedParameters ?? new List<SelectedParameterDto>();

                if (string.IsNullOrEmpty(runReportApi.UKey))
                {
                    selectedParameters = runReportApi.Json.SelectedParameters;
                }
                if (runReportApi.IsDefaultFilter)
                    selectedParameters = runReportApi.Json.SelectedParameters;

                var dontSubTotal = runReportApi.Json.SelectedFields.Select(x => x.DontSubTotal);

                var reportInfoDto = runReportApi.Json.ReportId == 0 ? new ReportInfoDto { ReportName = string.Empty, BaseXrmEntityId = runReportApi.BaseReportXrmEntityId }
                                    : await _repository.GetReportNameAsync(id: runReportApi.Json.ReportId, cancellationToken: cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();

                var runReportContext = new RunReportDto
                {
                    Data = resultSet,
                    UKey = runReportApi.UKey,
                    OrderByClause = orderByClause,
                    GroupFunctions = runReportApi.Json.GroupFunctionList,
                    SelectedFields = runReportApi.Json.SelectedFields,
                    StoredProcId = runReportApi.Json.StoredProcId,
                    SelectedParameters = selectedParameters,
                    IsSubTotal = dontSubTotal,
                    IncludeSubTotal = runReportApi.Json.IncludeSubTotals,
                    IsScheduledReport = runReportApi.IsScheduledReport,
                    BaseEntityId = reportInfoDto.BaseXrmEntityId,                    
                    DrillDownRow = runReportApi.Json.DrillDownRow,
                    IsChart = isChart,
                    OutputTypeId = runReportApi.OutputTypeId,
                    OnlyTop = runReportApi.Json.OnlyTop,
                    SelectedFieldIds = selectedFieldIds ?? new List<int?>(),
                    SelectedSorts = selectedSorts,
                    ReportSettings = runReportApi.Json.ReportSettings
                };

                var runReport = await RunReport(runReportContext, sortByDefault, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                runReport.ReportName = reportInfoDto.ReportName;

                return new XrmSuccessWithDataResponse(data: runReport);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                var originalMessage = ex.Message;
                string queryPart = string.Empty;

                if (originalMessage.Contains('$'))
                {
                    queryPart = originalMessage.Split('$')[1]; // Extract everything after '$'
                }

                string modifiedMessage = originalMessage.StartsWith("The multi-part identifier")
                    ? $"{ReportConstant.TheMultiPartIdentifierError} ${queryPart}"
                    : $"{originalMessage} ${queryPart}";

                return new XrmInternalServerErrorResponse(modifiedMessage, ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public (string PivotColumn, string PivotFunction) PreparePivotDatas(List<GroupFunctionDto> columns)
        {
            var pivotColumn = columns.FirstOrDefault(x => x.GroupFunc == "Pivot");
            string pivotFunction = string.Empty;

            if (pivotColumn != null)
            {
                int pivotColumnIndex = columns.FindIndex(x => x.GroupFunc == "Pivot");

                if (pivotColumnIndex >= 0 && pivotColumnIndex < columns.Count - 1)
                {
                    var nextValue = columns[pivotColumnIndex + 1];
                    pivotFunction = nextValue.GroupFunc;
                }
            }

            return (
                 pivotColumn?.CustomLabel ?? string.Empty,
                 pivotColumn != null && !string.IsNullOrEmpty(pivotFunction) ? pivotFunction : string.Empty
             );
        }

        /// <summary>
        /// This method prepares the pivot data by extracting the pivot column and its associated function from the provided list of group functions.
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public (string PivotColumn, string PivotFunction) PreparePivotData(List<GroupFunctionDto> columns, CancellationToken cancellationToken = default)
        {
            try
            {
                var pivotColumn = columns.FirstOrDefault(x => x.GroupFunc == "Pivot");
                string pivotFunction = string.Empty;

                if (pivotColumn != null)
                {
                    int pivotIndex = columns.IndexOf(pivotColumn);

                    // Iterate from the next index to find the first non-disabled column
                    for (int i = pivotIndex + 1; i < columns.Count; i++)
                    {
                        if (!columns[i].Disabled)
                        {
                            pivotFunction = columns[i].GroupFunc;
                            break;
                        }
                    }
                }

                return (
                    pivotColumn?.CustomLabel ?? string.Empty,
                    pivotColumn != null && !string.IsNullOrEmpty(pivotFunction) ? pivotFunction : string.Empty
                );
            }
            catch (OperationCanceledException)
            {

                throw;
            }
        }

        /// <summary>
        /// Asynchronously retrieves a list of procedure tables based on the provided stored procedure ID.
        /// </summary>
        /// <param name="procId">The ID of the stored procedure for which the tables are to be retrieved.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        /// A task representing the asynchronous operation. The task result contains a list of <see cref="ProcedureTableDto"/> objects
        /// representing the tables associated with the specified stored procedure.
        /// </returns>        
        private async Task<List<ProcedureTableDto>> GetProcTablesAsync(int? procId, CancellationToken cancellationToken = default)
        {
            try
            {

                var settings = XrmReportSettings.GetSettings(cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();

                var payload = JsonSerializer.Serialize(new
                {
                    storedProcId = procId,
                    adminMode = true,
                    buildSql = false,
                });
                var procStringResult = await XrmCallReportApi.ExecuteCallReportApi(ReportConstant.UrlGetProcedures, payload, settings, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                var result = JsonSerializer.Deserialize<List<ProcedureTableDto>>(procStringResult);
                cancellationToken.ThrowIfCancellationRequested();

                return result;
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        /// <summary>
        /// Modifies the given SQL query by replacing the value of the @JsonParams parameter with a JSON string
        /// containing the provided page number, page size, and record IDs.
        /// </summary>
        /// <param name="sql">The SQL query to modify.</param>
        /// <param name="pageNumber">The page number to include in the JSON parameters.</param>
        /// <param name="pageSize">The page size to include in the JSON parameters.</param>
        /// <param name="spDar">The list of record IDs to include in the JSON parameters.</param>
        /// <param name="selectedParameters">The list of selected parameters to include in the JSON parameters.</param>
        /// <param name="isCount">A flag indicating whether the query is for counting records.</param>
        /// <param name="reportOwnerId">The owner of report</param>
        /// <param name="drillDownRow">The list of drill-down row items to include in the JSON parameters.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <param name="offsettime">The offset time.</param>
        /// <returns>The modified SQL query with the updated @JsonParams parameter.</returns>
        private string ModifySqlWithJsonParamsAsync(string sql, int pageNumber, int pageSize, bool isCount,
            Dictionary<string, IEnumerable<int>> spDar, List<SelectedParameterDto> selectedParameters, int reportOwnerId,
            List<ReportDataRowItemModelDto> drillDownRow, int offsettime, CancellationToken cancellationToken = default)
        {
            try
            {
                var paramList = selectedParameters.Select(x => new Param
                {
                    Operator = x.Operator,
                    Values = x.Value
                }).ToList();

                // Split SQL into two parts: before "@" and parameters
                int atIndex = sql.IndexOf('@');
                string prefix = sql.Substring(0, atIndex).Trim();
                string parameters = sql.Substring(atIndex).Trim();

                //var paramPattern = new Regex(@"@(\w+)\s*=\s*'(.*?)'", RegexOptions.Singleline);
                var paramPattern = new Regex(@"@(\w+)\s*=\s*'((?:[^']|'')*)'", RegexOptions.Singleline);

                var matches = paramPattern.Matches(parameters);

                var keyValuePairs = new Dictionary<string, string>();

                foreach (Match match in matches)
                {
                    string key = match.Groups[1].Value;   // Parameter name (e.g., JsonParams)
                    string value = match.Groups[2].Value;
                    keyValuePairs[key] = value;
                }

                var jsonParams = new JsonParamsDto
                {
                    Params = new List<Dictionary<string, Param>>()
                };

                // Add selected parameters to JsonParams
                int index = 0;
                foreach (var kvp in keyValuePairs)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (kvp.Key.Equals("JsonParams", StringComparison.OrdinalIgnoreCase))
                        continue;

                    var param = index < paramList.Count
                        ? paramList[index]
                        : new Param { Operator = string.Empty, Values = string.Empty };

                    jsonParams.Params.Add(new Dictionary<string, Param>
                {
                    { kvp.Key, new Param
                        {
                            Operator = param.Operator,
                            Values = param.Values?.Replace("'","''")
                        }
                    }
                });

                    index++;
                }

                jsonParams = AddSystemParam(jsonParams, "PageNumber", pageNumber.ToString());
                jsonParams = AddSystemParam(jsonParams, "PageSize", pageSize.ToString());
                jsonParams = AddSystemParam(jsonParams, "IsCount", isCount.ToString().ToLower());
                jsonParams = AddSystemParam(jsonParams, "ReportOwnerId", reportOwnerId.ToString());
                jsonParams = AddSystemParam(jsonParams, "Offsettime", offsettime.ToString());
                if (drillDownRow != null && drillDownRow.Any())
                {
                    var drillValues = drillDownRow
                                     .Select(d => d.Value?.ToString()?.Replace("'", "''"))
                                     .Where(v => !string.IsNullOrEmpty(v))
                                     .Select(v => $"{{{v}}}");

                    string drillValueString = string.Join(",", drillValues);

                    jsonParams.Params.Add(new Dictionary<string, Param>
                {
                    { "Drilldown", new Param { Operator = string.Empty, Values = drillValueString } }
                });
                    jsonParams.Params.Add(new Dictionary<string, Param>
                {
                    { "IsDrilldown", new Param { Operator = string.Empty, Values = "true" } }
                });
                }
                else
                {
                    jsonParams.Params.Add(new Dictionary<string, Param>
                {
                    { "Drilldown", new Param { Operator = string.Empty, Values = string.Empty } }
                });
                    jsonParams.Params.Add(new Dictionary<string, Param>
                {
                    { "IsDrilldown", new Param { Operator = string.Empty, Values = "false" } }
                });
                }

                //foreach (var kvp in spDar)
                //{
                //    jsonParams.Params.Add(new Dictionary<string, Param>
                //{
                //    { kvp.Key, new Param { Operator = string.Empty, Values = string.Join(",", kvp.Value) } }
                //});
                //}

                string jsonParamsString = Newtonsoft.Json.JsonConvert.SerializeObject(jsonParams);

                if (keyValuePairs.ContainsKey("JsonParams"))
                {
                    keyValuePairs["JsonParams"] = jsonParamsString;
                }

                List<string> modifiedParams = new List<string>();

                foreach (var kvp in keyValuePairs)
                {
                    var safeValue = kvp.Value?.Replace("'", "''");
                    modifiedParams.Add($"@{kvp.Key} = '{safeValue}'");
                }

                sql = prefix + " " + string.Join(", ", modifiedParams);
                return sql;
            }
            catch (OperationCanceledException)
            {
                return ReportConstant.ReportTimeOutMessage;
            }
        }
        private JsonParamsDto AddSystemParam(JsonParamsDto jsonParams, string key, string value)
        {
            jsonParams.Params.Add(new Dictionary<string, Param>
            {
                { key, new Param { Operator = string.Empty, Values = value }}
            });
            return jsonParams;
        }

        /// <summary>
        /// Modifies the given SQL query to group chart data by Year, Month, and Day, and applies ordering.
        /// </summary>
        /// <param name="query">The SQL query to be modified.</param>       
        /// <param name="visualizationType"></param>
        /// <param name="groupFunctions">The list of group functions applied.</param>
        /// <param name="selectedSorts"></param>
        /// <param name="drillDownRow"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The modified SQL query with grouping and ordering applied.</returns>
        /// <exception cref="ArgumentException">Thrown if no table and column names are found in the query.</exception>
        public async ValueTask<string> ApplyChartGroupingByDate(string query, string visualizationType, List<GroupFunctionDto> groupFunctions, List<SelectedSortDto> selectedSorts, List<ReportDataRowItemModelDto> drillDownRow, CancellationToken cancellationToken = default)
        {
            try
            {

                if (drillDownRow != null && drillDownRow?.Any() == true)
                    return query;

                var applicableGroupFunctions = new List<string> { "Group by Month", "Group by Year", "Group by Month/Year" };

                //var sqlFields = XrmDotNetReportHelper.SplitSqlColumns(query);
                var sqlFields = XrmDotNetReportHelper.SplitSqlColumns(query)
                                                     .Select(f =>
                                                     {
                                                         var trimmed = f.Trim();
                                                         // Remove "TOP <number>" if present at the start
                                                         trimmed = Regex.Replace(trimmed, @"^\s*TOP\s*\(?\d+\)?\s+", "", RegexOptions.IgnoreCase);
                                                         return trimmed;
                                                     })
                                                     .ToList();

                string firstColumn = sqlFields.FirstOrDefault()?.Trim();

                string firstColumnAlias = null;
                if (!string.IsNullOrEmpty(firstColumn))
                {
                    // Split on 'AS' (case-insensitive), and take the alias part
                    var aliasSplit = Regex.Split(firstColumn, @"\s+AS\s+", RegexOptions.IgnoreCase);
                    if (aliasSplit.Length == 2)
                    {
                        firstColumnAlias = aliasSplit[1].Trim().Trim('[', ']'); // Removes brackets if present
                    }
                    else
                    {
                        // If no alias is used, fall back to column name (e.g., [Table].[Column])
                        var parts = firstColumn.Split('.');
                        firstColumnAlias = parts.LastOrDefault()?.Trim().Trim('[', ']');
                    }
                }

                var firstGroupFunc = groupFunctions.FirstOrDefault(gf =>
                                                    !string.IsNullOrEmpty(gf.CustomLabel) && firstColumnAlias != null &&
                                                    firstColumnAlias.Contains(gf.CustomLabel, StringComparison.OrdinalIgnoreCase) &&
                                                    applicableGroupFunctions.Contains(gf.GroupFunc));

                if (firstGroupFunc != null)
                {
                    //Regex to find the first column in [TableName].[ColumnName] format

                    //string pattern = @"(?:\w+\()?(\[[^\]]+\])\.(\[[^\]]+\])";
                    //string pattern = @"(?:\w+\()?(\[[^\]]+\])\.(\[[^\]]+\])(?:\.(\[[^\]]+\]))?";

                    string pattern = @"(?:\w+\()?(\[[^\]]+\])\.(\[[^\]]+\])(?:\.(\[[^\]]+\]))?";
                    Match match = Regex.Match(query, pattern);

                    if (match.Success)
                    {
                        string first = match.Groups[1].Value;   // [rpt]
                        string second = match.Groups[2].Value;  // [Assignment_TerminationDetails]
                        string third = match.Groups[3].Value;   // [TerminationDate] (if present)

                        string tableWithBrackets;
                        string columnWithBrackets;

                        // ✅ If third group (column) exists, use schema+table and column
                        if (!string.IsNullOrEmpty(third))
                        {
                            tableWithBrackets = $"{first}.{second}";
                            columnWithBrackets = third;
                        }
                        else
                        {
                            // ✅ Fallback: if the two parts are identical, use only one (avoid duplicates)
                            if (string.Equals(first, second, StringComparison.OrdinalIgnoreCase))
                            {
                                tableWithBrackets = first; // use only once
                            }
                            else
                            {
                                tableWithBrackets = first; // treat as table
                            }

                            columnWithBrackets = second;
                        }

                        //string tableWithBrackets = match.Groups[2].Value;  // Extract [TableName]                        

                        //string columnWithBrackets = match.Groups[3].Success ? match.Groups[3].Value : match.Groups[2].Value;

                        List<string> newColumnsList = new List<string>
                                {
                                    $"YEAR({tableWithBrackets}.{columnWithBrackets}) AS YearNumber",
                                    $"MONTH({tableWithBrackets}.{columnWithBrackets}) AS MonthNumber"
                                };


                        string newColumns = string.Join(", ", newColumnsList) + ", ";

                        var (columnIdentifiers, fullyQualifiedDateTimeColumns) = await BuildDateTimeColumnInfoAsync();

                        var offsetTime = UserInfo.GetOffsetMinutes();

                        newColumns = ApplyTimeZoneOffsetToSql(newColumns, fullyQualifiedDateTimeColumns, offsetTime);

                        int columnIndex = query.IndexOf(firstColumn, StringComparison.OrdinalIgnoreCase);
                        if (columnIndex != -1)
                        {
                            // Get everything before the first column
                            string preColumnsText = query.Substring(0, columnIndex);

                            // Get the part of the query after the first column
                            string postColumnsText = query.Substring(columnIndex);

                            query = preColumnsText + " " + newColumns + postColumnsText;
                        }

                        // Ensure GROUP BY includes Year, Month, Day (without duplicates)
                        string newGroupBy = $"YEAR({tableWithBrackets}.{columnWithBrackets}), " +
                                            $"MONTH({tableWithBrackets}.{columnWithBrackets})";

                        newGroupBy = ApplyTimeZoneOffsetToSql(newGroupBy, fullyQualifiedDateTimeColumns, offsetTime);

                        if (!Regex.IsMatch(query, @"(?i)GROUP\s+BY\s+.*?" + Regex.Escape(newGroupBy), RegexOptions.IgnoreCase))
                        {
                            query = Regex.Replace(query, @"(?i)GROUP\s+BY\s+", match => match.Value + newGroupBy + ", ", RegexOptions.IgnoreCase);
                        }
                        var matchingSort = selectedSorts.FirstOrDefault(s => groupFunctions.Any(g => g.FieldId == s.FieldId));

                        // Set by default ASC if there is no matching sort
                        string yearSortOrder = "ASC";
                        string monthSortOrder = "ASC";

                        if (matchingSort != null && matchingSort.Descending)
                        {
                            yearSortOrder = "DESC";
                            monthSortOrder = "DESC";
                        }
                        
                        string newOrderBy = string.Empty;
                        if (string.Equals(visualizationType, nameof(OutputType.Summary), StringComparison.OrdinalIgnoreCase))
                        {
                            newOrderBy = $"ORDER BY YEAR({tableWithBrackets}.{columnWithBrackets}) {yearSortOrder}, " +
                                           $"MONTH({tableWithBrackets}.{columnWithBrackets}) {monthSortOrder} ";
                        }
                        else
                        {
                            // Ensure ORDER BY always follows Year → Month → Day
                            newOrderBy = $"ORDER BY YEAR({tableWithBrackets}.{columnWithBrackets}) Desc, " +
                                               $"MONTH({tableWithBrackets}.{columnWithBrackets}) Desc ";
                        }
                        newOrderBy = ApplyTimeZoneOffsetToSql(newOrderBy, fullyQualifiedDateTimeColumns, offsetTime);

                        string orderByPattern = @"ORDER\s+BY\s+.*?(OFFSET\s+\d+\s+ROWS\s+FETCH\s+NEXT\s+\d+\s+ROWS\s+ONLY)?;?$";
                        Match orderByMatch = Regex.Match(query, orderByPattern, RegexOptions.IgnoreCase);

                        if (orderByMatch.Success)
                        {
                            // Replace existing ORDER BY
                            query = Regex.Replace(query, orderByPattern, newOrderBy + (orderByMatch.Groups[1].Success ? " " + orderByMatch.Groups[1].Value : "") + ";", RegexOptions.IgnoreCase);
                        }
                        else
                        {
                            // Append new ORDER BY if none exists
                            query += "\n" + newOrderBy + ";";
                        }
                        // Wrap the query inside a subquery to sort on YearNumber and MonthNumber
                        query = $"SELECT * FROM (\n{query.Trim().TrimEnd(';')}\n) AS months ORDER BY YearNumber {yearSortOrder}, MonthNumber {monthSortOrder};";

                    }
                }
                cancellationToken.ThrowIfCancellationRequested();

                return query;
            }
            catch (OperationCanceledException)
            {

                return null;
            }
        }

        private string ExtractTopClause(string sql, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var match = Regex.Match(sql, @"SELECT\s+(?:DISTINCT\s+)?TOP\s+\d+", RegexOptions.IgnoreCase);
                if (match.Success)
                {
                    var matchedText = match.Value;
                    var topMatch = Regex.Match(matchedText, @"TOP\s+\d+", RegexOptions.IgnoreCase);
                    return topMatch.Success ? topMatch.Value : string.Empty;
                }
                return string.Empty;
            }
            catch (OperationCanceledException)
            {
                return ReportConstant.ReportTimeOutMessage;
            }
        }

        private async Task<List<TableDto>> GetTablesAsync()
        {
            // Use cache for tables list
            var cacheKey = _cacheKeyService.DeriveGlobalSpecificKey<XrmRunReportService>("Tables");
            var cacheResult = await _cacheService.TryGetAsync<List<TableDto>>(cacheKey);
            if (cacheResult.Success && cacheResult.Value != null)
                return cacheResult.Value;

            var response = await XrmCallReportApi.ExecuteCallReportApi(ReportConstant.UrlGetTables, "{\"adminMode\":false}");
            var tables = JsonSerializer.Deserialize<List<TableDto>>(response) ?? new List<TableDto>();

            // Cache tables for a reasonable TTL
            _cacheService.Set(cacheKey, tables);
            return tables;
        }

        /// <summary>
        /// Method to apply timezone offset to DateTime columns in the provided SQL query.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="fullyQualifiedDateTimeColumns"></param>
        /// <param name="offsetMinutes"></param>
        /// <returns></returns>
        public static string ApplyTimeZoneOffsetToSqlReplacement(string sql, HashSet<string> fullyQualifiedDateTimeColumns, int offsetMinutes)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return sql;

            // Avoid double replacement (check if already wrapped)
            if (sql.Contains("DATEADD(MINUTE", StringComparison.OrdinalIgnoreCase))
            {
                // protect previously replaced columns temporarily
                sql = Regex.Replace(sql, @"DATEADD\(MINUTE,\s*[-+]?\d+,\s*(\[.*?\])\)", "##TZ_PROTECT##$1##", RegexOptions.IgnoreCase);
            }

            var regex1 = new Regex(@"\[\s*([A-Za-z0-9_]+)\s*\]\.\[\s*([A-Za-z0-9_]+)\s*\]", RegexOptions.IgnoreCase);
            // 1-part replacement (table + column)
            sql = regex1.Replace(sql, match =>
            {
                string table = match.Groups[1].Value;
                string column = match.Groups[2].Value;

                // Try to find matching schema
                var matchFqn = fullyQualifiedDateTimeColumns
                    .FirstOrDefault(fqn => fqn.EndsWith($".{table.ToLowerInvariant()}.{column.ToLowerInvariant()}"));

                if (matchFqn != null)
                {
                    var parts = matchFqn.Split('.');
                    string schema = parts[0];

                    return $"DATEADD(MINUTE, {offsetMinutes}, [{schema}].[{table}].[{column}])";
                }

                return match.Value;
            });

            var regex2 = new Regex(@"\[\s*([A-Za-z0-9_]+)\s*\]\.\[\s*([A-Za-z0-9_]+)\s*\]\.\[\s*([A-Za-z0-9_]+)\s*\]", RegexOptions.IgnoreCase);

            // 2-part replacement (schema + table + column)
            sql = regex2.Replace(sql, match =>
            {
                string schema = match.Groups[1].Value;
                string table = match.Groups[2].Value;
                string column = match.Groups[3].Value;

                string identifier = $"{schema}.{table}.{column}".ToLowerInvariant();

                if (fullyQualifiedDateTimeColumns.Contains(identifier))
                {
                    return $"DATEADD(MINUTE, {offsetMinutes}, [{schema}].[{table}].[{column}])";
                }

                return match.Value;
            });
            // Restore protected columns (avoid double DATEADD)
            sql = sql.Replace("##TZ_PROTECT##", "DATEADD(MINUTE, -300, ");
            return sql;
        }

        /// <summary>
        /// Applies a DATEADD-based timezone offset to all DateTime columns found in a SQL string.
        /// Replaces fully qualified or table-qualified column names only once, across SELECT, WHERE, GROUP BY, etc.
        /// </summary>
        /// <param name="sql">The original SQL query text.</param>
        /// <param name="fullyQualifiedDateTimeColumns">
        /// A set of fully qualified column identifiers (schema.table.column) to apply the offset to.
        /// </param>
        /// <param name="offsetMinutes">The timezone offset in minutes (can be negative).</param>
        /// <returns>Modified SQL query with DATEADD applied to DateTime fields.</returns>
        public static string ApplyTimeZoneOffsetToSql(
            string sql,
            HashSet<string> fullyQualifiedDateTimeColumns,
            int offsetMinutes)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return sql;

            // Avoid double replacement (check if already wrapped)
            if (sql.Contains("DATEADD(MINUTE", StringComparison.OrdinalIgnoreCase))
            {
                // protect previously replaced columns temporarily
                sql = Regex.Replace(sql, @"DATEADD\(MINUTE,\s*[-+]?\d+,\s*(\[.*?\])\)", "##TZ_PROTECT##$1##", RegexOptions.IgnoreCase);
            }

            // Pattern for schema.table.column like [rpt].[Sector_BasicDetails].[CreatedOn]
            var regexFqn = new Regex(@"\[\s*([A-Za-z0-9_]+)\s*\]\.\[\s*([A-Za-z0-9_]+)\s*\]\.\[\s*([A-Za-z0-9_]+)\s*\]", RegexOptions.IgnoreCase);

            // Pattern for table.column like [Sector_BasicDetails].[CreatedOn]
            var regexTwoPart = new Regex(@"\[\s*([A-Za-z0-9_]+)\s*\]\.\[\s*([A-Za-z0-9_]+)\s*\]", RegexOptions.IgnoreCase);

            // Handle 3-part identifiers first
            sql = regexFqn.Replace(sql, match =>
            {
                string schema = match.Groups[1].Value;
                string table = match.Groups[2].Value;
                string column = match.Groups[3].Value;
                string identifier = $"{schema}.{table}.{column}".ToLowerInvariant();

                if (fullyQualifiedDateTimeColumns.Contains(identifier))
                {
                    return $"DATEADD(MINUTE, {offsetMinutes}, [{schema}].[{table}].[{column}])";
                }

                return match.Value;
            });

            // Handle 2-part identifiers (when schema missing in WHERE clause)
            sql = regexTwoPart.Replace(sql, match =>
            {
                string table = match.Groups[1].Value;
                string column = match.Groups[2].Value;

                var matchFqn = fullyQualifiedDateTimeColumns
                    .FirstOrDefault(fqn => fqn.EndsWith($".{table.ToLowerInvariant()}.{column.ToLowerInvariant()}"));

                if (matchFqn != null)
                {
                    var schema = matchFqn.Split('.')[0];
                    return $"DATEADD(MINUTE, {offsetMinutes}, [{schema}].[{table}].[{column}])";
                }

                return match.Value;
            });

            // Restore protected DATEADD calls
            sql = Regex.Replace(
                sql,
                @"##TZ_PROTECT##(.*?)##",
                "$1",
                RegexOptions.Singleline);

            sql = ReplaceDateTimeInWhereClause(sql);

            return sql;
        }

        public static string ReplaceDateTimeInWhereClause(string sqlQuery)
        {
            if (string.IsNullOrWhiteSpace(sqlQuery)) return sqlQuery;
            string keyword = "WHERE";
            int whereIndex = sqlQuery.IndexOf(keyword, StringComparison.OrdinalIgnoreCase);

            if (whereIndex == -1) { return sqlQuery; }

            string[] clause = { "GROUP BY", "ORDER BY", "HAVING" };
            int endIndex = sqlQuery.Length;
            foreach (var term in clause)
            {
                int termIndex = sqlQuery.IndexOf(term, whereIndex, StringComparison.OrdinalIgnoreCase);
                // If we found a terminator and it is closer than the current endIndex, update it
                if (termIndex != -1 && termIndex < endIndex)
                {
                    endIndex = termIndex;
                }
            }

            string beforeWhere = sqlQuery.Substring(0, whereIndex);

            // Part B: The Where Clause itself (From WHERE up to the next clause)
            string whereClause = sqlQuery.Substring(whereIndex, endIndex - whereIndex);

            // Part C: Everything AFTER the Where clause (Group By, Order By, etc.)
            string afterWhere = sqlQuery.Substring(endIndex);

            // 4. Perform replacement ONLY on Part B (The Where Clause)
            string modifiedWhere = whereClause.Replace("CONVERT(DATETIME,", "CONVERT(DATE,");

            // 5. Rejoin the parts
            return beforeWhere + modifiedWhere + afterWhere;
        }

        /// <summary>
        /// Extracts DateTime field metadata from the selected report fields,
        /// builds fully-qualified column identifiers using schema, table, and 
        /// column names, and returns both the raw identifier list and a 
        /// HashSet of normalized fully-qualified DateTime column identifiers 
        /// to be used for SQL timezone transformation.
        /// </summary>        
        /// <returns>
        /// A tuple containing:
        ///   1. columnIdentifiers: List of ColumnIdentifierDto with schema, table, and column metadata.
        ///   2. fullyQualifiedDateTimeColumns: HashSet of normalized fully qualified names 
        ///      (schema.table.column) for efficient SQL replacement.
        /// </returns>
        /// <remarks>
        /// This method queries field definitions per table via the Report API,
        /// filters only the DateTime fields, and resolves table metadata via GetTablesAsync().
        /// It is used together with ApplyTimeZoneOffsetToSql for timezone-adjusted SQL generation.
        /// </remarks>
        public async ValueTask<(List<ColumnIdentifierDto> columnIdentifiers, HashSet<string> fullyQualifiedDateTimeColumns)> BuildDateTimeColumnInfoAsync()
        {
            var cacheKey = _cacheKeyService.DeriveGlobalSpecificKey<XrmRunReportService>(CacheKeyEnum.GetReportFields.ToString());
            var cacheResult = await _cacheService.TryGetAsync<(List<ColumnIdentifierDto>, HashSet<string>)>(cacheKey);

            if (cacheResult.Success && cacheResult.Value.Item1 != null && cacheResult.Value.Item2 != null)
            {
                return cacheResult.Value;
            }

            var tablesMap = (await GetTablesAsync()).ToDictionary(t => t.tableId);

            var tableIdsList = tablesMap.Keys.ToList().Distinct();

            var tasks = tableIdsList.Select(async tableId =>
            {
                var fieldCacheKey = _cacheKeyService.DeriveGlobalSpecificKey<XrmRunReportService>($"Fields_{tableId}");
                var cachedFields = await _cacheService.TryGetAsync<List<FieldDto>>(fieldCacheKey);
                if (cachedFields.Success && cachedFields.Value != null)
                    return cachedFields.Value;

                var payload = JsonSerializer.Serialize(new { tableId });

                var response = await XrmCallReportApi.ExecuteCallReportApi(ReportConstant.UrlGetFields, payload);
                var list = JsonSerializer.Deserialize<List<FieldDto>>(response) ?? new List<FieldDto>();

                _cacheService.Set(fieldCacheKey, list);
                return list;
            });

            var allFieldLists = await Task.WhenAll(tasks);

            // 1. Filter DateTime fields only
            var dateTimeFields = allFieldLists
                                 .SelectMany(f => f)
                                 .Where(f => f.fieldType == "DateTime")
                                 .ToList();

            // 2. Build ColumnIdentifierDto list
            var columnIdentifiers = new List<ColumnIdentifierDto>();

            foreach (var field in dateTimeFields)
            {
                if (tablesMap.TryGetValue(field.tableId, out var table))
                {
                    columnIdentifiers.Add(new
                        ColumnIdentifierDto(table.schemaName, table.tableDbName, field.fieldDbName));
                }
            }

            var fullyQualifiedDateTimeColumns = columnIdentifiers
                .Select(c => c.ToStringIdentifier())
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var data = (columnIdentifiers ?? new List<ColumnIdentifierDto>(), fullyQualifiedDateTimeColumns ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase));

            _cacheService.Set(cacheKey, data);

            return (columnIdentifiers, fullyQualifiedDateTimeColumns);
        }

        /// <summary>
        /// Executes the provided report query, handles pagination, sorting, and pivoting operations,         
        /// </summary>
        /// <param name="runReport">The parameters required to run the report.</param>  
        /// <param name="selectedSorts"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>An instance of ResponseBase indicating the result of the operation.
        /// An HTTP response containing the list of all folders.
        /// A <see cref="XrmSuccessResponse"/> object if request is successful.
        /// A <see cref="XrmInternalServerErrorResponse"/> object if some internal exception occurs. Check Error Log.
        /// </returns>
        private async ValueTask<XrmDotNetReportResultModel> RunReport(RunReportDto runReport, List<SelectedSortDto> selectedSorts, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DBHelper db = new DBHelper();
            DBHelper dbForParallelExecution = new DBHelper();
            string reportSql = runReport.Data.Sql;
            string connectKey = runReport.Data.ConnectKey;
            string visualizationType = runReport.Data.VisualizationType;
            int pageNumber = runReport.Data.PageNumber > 0 ? runReport.Data.PageNumber : 1;
            int pageSize = runReport.Data.PageSize;
            string sortBy = runReport.Data.SortingDirection;
            bool desc = runReport.Data.Desc;
            string reportSeries = runReport.Data.ReportSeries;
            string pivotColumn = runReport.Data.PivotColumn;
            string pivotFunction = runReport.Data.PivotFunction;
            string reportData = runReport.Data.ReportData;
            bool subtotalMode = runReport.Data.SubTotalMode;
            var sql = string.Empty;
            var sqlCount = string.Empty;
            int totalRecords = 0;
            var sqlDar = (string.Empty, new Dictionary<string, IEnumerable<int>>(), string.Empty);
            var spDar = new Dictionary<string, IEnumerable<int>>();
            var isApplyDistinctInSql = true;
            int onlyTop = runReport.OnlyTop ?? 0;

            var configureClient = _serviceProvider.GetService<IConfigureClientRepository>();
            var organizationLabel = await configureClient.GetBasicInfoAsync(cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                if (string.IsNullOrEmpty(reportSql))
                {
                    throw new Exception(ReportConstant.QueryNotFound);
                }

                var allSqls = reportSql.Split(new string[] { "%2C", "," }, StringSplitOptions.RemoveEmptyEntries);
                var dtPaged = new DataTable();
                var dtCols = 0;
                int executedBy = 0;

                List<string> fields = new List<string>();
                List<string> sqlFields = new List<string>();
                string pattern = string.Empty;

                string replacement = "[rpt].[$1]";
                pattern = @"(?<=\bFrom\s+)(?!\[rpt\]\.)\[([^\]]+)\]";
                var decodeOps = new[] { StringOperation.IdToSquareBracketHash, StringOperation.CommaToSquareBracket };

                if (runReport.IsScheduledReport)
                {
                    executedBy = await _xrmReportCommonRepository.GetOwnerIdAsync(runReport.UKey, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                }
                else
                {
                    executedBy = _loggedInUserNo;
                }

                for (int i = 0; i < allSqls.Length; i++)
                {
                    string rawSql = HttpUtility.HtmlDecode(allSqls[i]);
                    string decryptedSql = XrmDotNetReportHelper.Decrypt(rawSql);

                    string updatedSql = Regex.Replace(decryptedSql, pattern, replacement, RegexOptions.IgnoreCase, TimeSpan.FromMilliseconds(100));

                    updatedSql = updatedSql.Replace("GetDate()", "CAST(GETDATE() As Date)", StringComparison.OrdinalIgnoreCase);
                    updatedSql = StringTransformer.DecodeMultiple(updatedSql, decodeOps);
                    updatedSql = updatedSql.Replace("{UserGroupId}", _loggedInUserGroupId.ToString());
                    updatedSql = RemoveColumnBySubstring(updatedSql, "__prm__");
                    sql = updatedSql;

                    var (columnIdentifiers, fullyQualifiedDateTimeColumns) = await BuildDateTimeColumnInfoAsync();

                    var offsetTime = UserInfo.GetOffsetMinutes();

                    Task<int> countTask = Task.FromResult(0);
                    Task<DataSet> dataSetTask = Task.FromResult<DataSet>(null);                    

                    if (!sql.StartsWith("EXEC"))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        int orderByIndex = sql.IndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase);

                        if (orderByIndex != -1)
                        {
                            string afterOrderBy = sql.Substring(orderByIndex + "ORDER BY".Length).Trim();
                            // If ORDER BY has only DESC/ASC, remove it
                            if (afterOrderBy.Equals("DESC", StringComparison.OrdinalIgnoreCase) ||
                                afterOrderBy.Equals("ASC", StringComparison.OrdinalIgnoreCase))
                            {
                                sql = sql.Substring(0, orderByIndex).Trim();
                            }

                            else if (!string.IsNullOrEmpty(runReport.OrderByClause))
                            {
                                // Replace existing ORDER BY with custom one
                                string beforeOrderBy = sql.Substring(0, orderByIndex);
                                sql = $"{beforeOrderBy} {runReport.OrderByClause}";
                            }
                        }
                        else if (!sql.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase))
                        {
                            sql = $"{sql.Trim()} {(string.IsNullOrEmpty(runReport.OrderByClause) ? "ORDER BY 1" : runReport.OrderByClause)}";
                        }

                        sqlDar = await XrmReportDataAccessService.ApplyDataAccessRightsOptimized(sql, _serviceProvider, executedBy,
                                                       false, runReport.BaseEntityId, cancellationToken);

                        cancellationToken.ThrowIfCancellationRequested();

                        sql = sqlDar.Item1;

                        var fromIndex = XrmDotNetReportHelper.FindFromIndex(sql);

                        sqlFields = XrmDotNetReportHelper.SplitSqlColumns(sql);

                        if (sql.Contains("DISTINCT", StringComparison.OrdinalIgnoreCase))
                        {
                            isApplyDistinctInSql = sqlFields.Count() <= 50;
                            if (!isApplyDistinctInSql)
                                sql = sql.Replace("DISTINCT", string.Empty);
                        }

                        string topClause = ExtractTopClause(sql, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();

                        var sqlFrom = sql.Contains("DISTINCT", StringComparison.OrdinalIgnoreCase)
                            ? $"SELECT DISTINCT {topClause} {string.Join(", ", sqlFields)} {sql.Substring(fromIndex)}"
                            : $"SELECT {topClause} {string.Join(", ", sqlFields)} {sql.Substring(fromIndex)}";

                        pattern = @"AS\s+\[(.*?)\]";
                        var matches = Regex.Matches(sqlFrom, pattern, RegexOptions.None, TimeSpan.FromMilliseconds(100));
                        var aliasOccurrences = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                        foreach (Match match in matches)
                        {
                            string alias = match.Groups[1].Value; // Extract alias name

                            if (aliasOccurrences.ContainsKey(alias))
                            {
                                aliasOccurrences[alias]++;
                                string newAlias = $"{alias}_{Guid.NewGuid():N}"; // Append unique key
                                sqlFrom = ReplaceFirst(sqlFrom, $"AS [{alias}]", $"AS [{newAlias}]", StringComparison.OrdinalIgnoreCase);
                            }
                            else
                            {
                                aliasOccurrences[alias] = 1;
                            }
                        }

                        string coreSql = sqlFrom.Contains("ORDER BY") ? sqlFrom.Substring(0, sqlFrom.IndexOf("ORDER BY")) : sqlFrom;

                        sqlCount = $@"
                        IF OBJECT_ID('tempdb..#CountTemp') IS NOT NULL DROP TABLE #CountTemp;
                        SELECT * INTO #CountTemp FROM ({coreSql}) AS src;
                        SELECT COUNT(*) FROM #CountTemp;
                        DROP TABLE #CountTemp;";

                        //sqlCount = $"SELECT COUNT(*) FROM ({(sqlFrom.Contains("ORDER BY") ? sqlFrom.Substring(0, sqlFrom.IndexOf("ORDER BY")) : sqlFrom)}) as countQry";

                        if (!String.IsNullOrEmpty(sortBy))
                        {
                            if (sortBy.StartsWith("DATENAME(MONTH, "))
                            {
                                sortBy = sortBy.Replace("DATENAME(MONTH, ", "MONTH(");
                            }
                            if (sortBy.StartsWith("MONTH(") && sortBy.Contains(")) +") && sql.Contains("Group By"))
                            {
                                sortBy = sortBy.Replace("MONTH(", "CONVERT(VARCHAR(3), DATENAME(MONTH, ");
                            }
                            if (!sql.Contains("ORDER BY"))
                            {
                                sql = sql + "ORDER BY " + sortBy + (desc ? " DESC" : "");
                            }
                            else
                            {
                                sql = sql.Substring(0, sql.IndexOf("ORDER BY")) + "ORDER BY " + sortBy + (desc ? " DESC" : "");
                            }
                        }

                        if (sql.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase)
                            && !sql.Contains(" TOP ", StringComparison.OrdinalIgnoreCase)
                            && isApplyDistinctInSql
                            && (string.IsNullOrEmpty(pivotColumn) || (runReport.DrillDownRow != null && runReport.DrillDownRow.Any())))
                        {
                            int offset = (pageNumber - 1) * pageSize;
                            sql = sql + $" OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY";
                        }

                        if (sql.Contains("__jsonc__"))
                            sql = sql.Replace("__jsonc__", "");

                        if (!string.IsNullOrEmpty(pivotColumn) && (runReport.DrillDownRow == null || !runReport.DrillDownRow.Any()))
                        {
                            sql = sql.Remove(sql.IndexOf("SELECT "), "SELECT ".Length).Insert(sql.IndexOf("SELECT "), "SELECT TOP (1) ");
                            sql = sql.Replace($"TOP {onlyTop}", string.Empty);
                        }
                        else
                        {
                            if (isApplyDistinctInSql)
                                if (!runReport.IsChart)
                                {
                                    countTask = dbForParallelExecution.ExecuteScalarWithTempTableAsync(sqlDar.Item3 + sqlCount, sqlDar.Item2, null, CommandType.Text, cancellationToken)
                                    .ContinueWith(t => Convert.ToInt32(t.Result));
                                    cancellationToken.ThrowIfCancellationRequested();
                                }
                        }

                        sql = ApplyTimeZoneOffsetToSql(sql, fullyQualifiedDateTimeColumns, offsetTime);
                        sql = await ApplyChartGroupingByDate(sql, visualizationType, runReport.GroupFunctions, selectedSorts, runReport.DrillDownRow, cancellationToken);

                        cancellationToken.ThrowIfCancellationRequested();
                    }



                    else if (runReport.StoredProcId.HasValue)
                    {
                        spDar = await XrmReportDataAccessService.ApplyDataAccessRightsForSP(sql, _serviceProvider, executedBy, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();

                        sql = ModifySqlWithJsonParamsAsync(sql, pageNumber, pageSize, false, spDar, runReport.SelectedParameters,
                            executedBy, runReport.DrillDownRow, offsetTime, cancellationToken);

                        sqlDar.Item2 = spDar;
                        cancellationToken.ThrowIfCancellationRequested();


                        if (!runReport.IsChart)
                        {
                            //var countParams = CloneParametersLocal(sqlDar.Item2.ToArray());
                            var sqlSPCount = ModifySqlWithJsonParamsAsync(sql, pageNumber, pageSize, true, spDar,
                                runReport.SelectedParameters, executedBy, runReport.DrillDownRow, offsetTime, cancellationToken);


                            cancellationToken.ThrowIfCancellationRequested();

                            countTask = dbForParallelExecution.ExecuteScalarWithTempTableAsync(sqlSPCount, spDar, null, CommandType.Text, cancellationToken)
                                .ContinueWith(t => Convert.ToInt32(t.Result));
                            cancellationToken.ThrowIfCancellationRequested();
                        }
                        else
                        {
                            totalRecords = 0;
                        }
                    }

                    var dtPagedRun = new DataTable();
                    var dataSet = new DataSet();
                    dataSetTask = db.ExecuteDataSetWithTempTableAsync(sql, sqlDar.Item2, null, CommandType.Text, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();

                    // Wait for both tasks to complete
                    await Task.WhenAll(countTask, dataSetTask);

                    // Get results
                    totalRecords = Math.Max(totalRecords, countTask.Result);

                    dataSet = dataSetTask.Result;

                    dtPagedRun = dataSet.Tables[0];


                    if (!isApplyDistinctInSql)
                    {
                        var result = ApplyDistinctWithPaging(dtPagedRun, pageSize, pageNumber, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();
                        dtPagedRun = result.Item1;
                        totalRecords = result.Item2;
                    }

                    dtPagedRun = RemoveBlankRows(dtPagedRun, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    if (runReport.StoredProcId.HasValue)
                    {
                        var procedureTables = await GetProcTablesAsync(runReport.StoredProcId, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();
                        dtPagedRun = ReorderAndFilterTable(dtPagedRun, runReport.GroupFunctions, procedureTables, runReport.StoredProcId.HasValue);
                    }

                    if (!sqlFields.Any())
                    {
                        foreach (DataColumn c in dtPagedRun.Columns)
                        {
                            sqlFields.Add($"{c.ColumnName} AS {c.ColumnName}");
                        }
                    }

                    string[] series = { };
                    if (i == 0)
                    {
                        fields.AddRange(sqlFields);

                        if (!string.IsNullOrEmpty(pivotColumn) && (runReport.DrillDownRow == null || !runReport.DrillDownRow.Any()))
                        {
                            PivotTableDto pivotTable = new PivotTableDto
                            {
                                DbHelper = db,
                                DataTable = dtPagedRun,
                                Sql = sqlDar,
                                SqlFields = sqlFields,
                                ReportDataJson = reportData,
                                PivotColumn = pivotColumn,
                                PivotFunction = pivotFunction,
                                PageNumber = pageNumber,
                                PageSize = pageSize,
                                SortBy = sortBy,
                                Descending = desc,
                                ReturnSubtotal = subtotalMode,
                                OrganizationLabel = organizationLabel.OrganizationLabel,
                                OnlyTop = onlyTop,
                                OrderByClause = runReport.OrderByClause,
                                GroupFunctions = runReport.GroupFunctions,
                                SelectedSorts = runReport.SelectedSorts,
                                SqlQuery = sql,
                                LoggedInUserGroupId = _loggedInUserGroupId
                            };

                            var pd = await XrmDotNetReportHelper.GetPivotTable(pivotTable, columnIdentifiers, fullyQualifiedDateTimeColumns);

                            dtPagedRun = pd.dt;
                            if (!string.IsNullOrEmpty(pd.sql)) sql = pd.sql;
                            totalRecords = pd.totalRecords;

                            // Extract existing field names (ignore aliases)
                            var existingFieldNames = new HashSet<string>(
                                fields.Select(f =>
                                {
                                    var match = Regex.Match(f, @"\bAS\s+(?:\[(.*?)\]|(.+))", RegexOptions.IgnoreCase);
                                    return match.Success
                                        ? (match.Groups[1].Success ? match.Groups[1].Value.Trim() : match.Groups[2].Value.Trim())
                                        : f.Trim();
                                }),
                                StringComparer.OrdinalIgnoreCase
                            );

                            // Build new fields list in exact DataTable order
                            var updatedFields = new List<string>();

                            //for (int j = 0; j < dtPagedRun.Columns.Count; j++)
                            //{
                            //    var col = dtPagedRun.Columns[j];
                            //    if (col.ColumnName.Contains(pivotTable.OrganizationLabel, StringComparison.OrdinalIgnoreCase))
                            //    {
                            //        col.ColumnName = col.ColumnName.Replace(pivotTable.OrganizationLabel, "Sector", StringComparison.OrdinalIgnoreCase);
                            //    }
                            //}

                            foreach (DataColumn col in dtPagedRun.Columns)
                            {
                                if (col.ColumnName.Contains(pivotTable.OrganizationLabel, StringComparison.OrdinalIgnoreCase))
                                {
                                    col.ColumnName = col.ColumnName.Replace(pivotTable.OrganizationLabel, "Sector", StringComparison.OrdinalIgnoreCase);
                                }

                                if (existingFieldNames.Contains(col.ColumnName))
                                {
                                    // Already in fields (keep original definition so aliasing is preserved)
                                    var existing = fields.First(f =>
                                    Regex.IsMatch(f, $@"\bAS\s+(\[?{Regex.Escape(col.ColumnName)}\]?)", RegexOptions.IgnoreCase));

                                    updatedFields.Add(existing);
                                }
                                else
                                {
                                    // New dynamic column from pivot → add fresh
                                    updatedFields.Add($"__ AS {col.ColumnName}");
                                }
                            }

                            fields = updatedFields;
                        }

                        dtPaged = dtPagedRun;
                        dtCols = dtPagedRun.Columns.Count;
                    }
                    else if (i > 0)
                    {
                        if (!string.IsNullOrEmpty(reportSeries))
                            series = reportSeries.Split(new string[] { "%2C", "," }, StringSplitOptions.RemoveEmptyEntries);

                        var j = 1;
                        while (j < dtPagedRun.Columns.Count)
                        {
                            var col = dtPagedRun.Columns[j++];
                            dtPaged.Columns.Add($"{col.ColumnName} ({series[i - 1]})", col.DataType);
                            fields.Add(sqlFields[j - 1]);
                        }

                        foreach (DataRow dr in dtPagedRun.Rows)
                        {
                            DataRow match = dtPaged.AsEnumerable().FirstOrDefault(drun => Convert.ToString(drun[0]) == Convert.ToString(dr[0]));
                            if (fields[0].ToUpper().StartsWith("CONVERT(VARCHAR(10)"))
                            {
                                match = dtPaged.AsEnumerable().Where(r => !string.IsNullOrEmpty(r.Field<string>(0)) && !string.IsNullOrEmpty((string)dr[0]) && Convert.ToDateTime(r.Field<string>(0)).Day == Convert.ToDateTime((string)dr[0]).Day).FirstOrDefault();
                            }
                            if (match != null)
                            {
                                j = 1;
                                while (j < dtPagedRun.Columns.Count)
                                {
                                    match[j + i + dtCols - 2] = dr[j];
                                    j++;
                                }
                            }
                            else
                            {
                                DataRow newRow = dtPaged.NewRow();
                                newRow[0] = dr[0];

                                j = 1;
                                while (j < dtPagedRun.Columns.Count)
                                {
                                    newRow[j + i + dtCols - 2] = dr[j];
                                    j++;
                                }

                                for (int k = 1; k < i + dtCols - 2; k++)
                                {
                                    newRow[k] = DBNull.Value;
                                }

                                dtPaged.Rows.Add(newRow);
                            }
                        }
                    }
                }

                if (dtPaged.Columns.Contains("YearNumber"))
                    dtPaged.Columns.Remove("YearNumber");

                if (dtPaged.Columns.Contains("MonthNumber"))
                    dtPaged.Columns.Remove("MonthNumber");

                var dataTableColumns = dtPaged.Columns.Cast<DataColumn>()
                                                   .Select(c => NormalizeColumnName(c.ColumnName))
                                                   .ToHashSet(StringComparer.OrdinalIgnoreCase);

                // 1. Get the list of all final column names in the correct order
                var dataTableColumnNames = dtPaged.Columns.Cast<DataColumn>()
                                                   .Select(c => c.ColumnName)
                                                   .ToList();

                var filteredSubTotalFlags = new List<bool>();

                if (!string.IsNullOrEmpty(pivotColumn))
                {
                    // 2. Pre-calculate a list of decoded pivot field names for matching
                    //    (Assuming 'Pivot' is the AggregateFunction value for pivoted fields)
                    var pivotFieldNames = runReport.SelectedFields
                                                   .Where(f => f.AggregateFunction == "Pivot")
                                                   .Select(f => StringOperationExtensions.Decode(f.FieldName, StringOperation.IdToSquareBracketHash))
                                                   .ToHashSet(StringComparer.OrdinalIgnoreCase);

                    // 3. Create a lookup dictionary for all SelectedFields for fast access by their decoded name
                    var selectedFieldLookup = runReport.SelectedFields.ToDictionary(
                        field => StringOperationExtensions.Decode(field.FieldName, StringOperation.IdToSquareBracketHash),
                        field => field,
                        StringComparer.OrdinalIgnoreCase
                    );

                    // 4. Helper Function to find the base field for a data table column
                    string GetBaseFieldNameForColumn(string currentColumnName, HashSet<string> pivotNames)
                    {
                        // First, try to see if this column belongs to a pivot field
                        foreach (var pivotName in pivotNames)
                        {
                            // Check if the pivot field name is a prefix of the column name.
                            // This is a common pattern (e.g., "Expense Type[Travel]")
                            if (currentColumnName.Equals(pivotName, StringComparison.OrdinalIgnoreCase))
                            {
                                return pivotName;
                            }
                        }
                        // If it's not a pivot column, assume the column name IS the field name (perhaps normalized already)
                        // We'll decode it to match the key in our lookup dictionary.
                        return StringOperationExtensions.Decode(currentColumnName, StringOperation.IdToSquareBracketHash);
                    }

                    // 5. Build the final list of flags by iterating over the DATA TABLE columns

                    foreach (var columnName in dataTableColumnNames)
                    {
                        // Find the root source field for this specific column
                        string baseFieldName = GetBaseFieldNameForColumn(columnName, pivotFieldNames);

                        // Find the corresponding report field configuration
                        if (selectedFieldLookup.TryGetValue(baseFieldName, out var foundField))
                        {
                            // Add the DontSubTotal flag from the source field
                            filteredSubTotalFlags.Add(foundField.DontSubTotal);
                        }
                        else
                        {
                            // This should not happen for columns we created. Default to FALSE (show subtotal) for safety.
                            filteredSubTotalFlags.Add(false);
                        }
                    }
                }
                else
                {
                    filteredSubTotalFlags = runReport.SelectedFields
                       .Where(field => dataTableColumns.Contains(StringOperationExtensions.Decode(field.FieldName, StringOperation.IdToSquareBracketHash)))
                       .Select(f => f.DontSubTotal)
                       .ToList();
                }

                runReport.OrganizationLabel = organizationLabel.OrganizationLabel;

                var reportDataDto = new ReportDataDto
                {
                    VisualizationType = visualizationType,
                    UKey = runReport.UKey,
                    GroupFunctions = runReport.GroupFunctions,
                    SelectedFields = runReport.SelectedFields,
                    SqlFields = fields,
                    DtPaged = dtPaged,
                    Sql = sql,
                    PageNumber = pageNumber,
                    PageSize = pageSize,
                    TotalRecords = totalRecords,
                    IsSubTotal = filteredSubTotalFlags,
                    IncludeSubTotal = runReport.IncludeSubTotal,
                    GetRunReport = runReport,
                    ReportSettings = runReport.ReportSettings

                };

                XrmDotNetReportResultModel model = await GetFilteredReportDataAsync(reportDataDto, runReport.StoredProcId, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();

                return model;
            }


            catch (OperationCanceledException)
            {
                throw;
            }

            catch (Exception ex)
            {
                throw new Exception($"{ex.Message} {ex.StackTrace} ${sql}", ex);
            }
        }

        private void LogInfo(string message)
        {
            System.IO.File.AppendAllText("Logs.txt", $"{Environment.NewLine}*******************{DateTime.Now}*******************{Environment.NewLine}INFO : {message}{Environment.NewLine}");
        }

        /// <summary>
        /// Applies distinct filtering to the rows of a DataTable and returns a paginated result.
        /// </summary>
        /// <param name="dataTable">The DataTable to process.</param>
        /// <param name="pageSize">The number of rows per page.</param>
        /// <param name="pageNumber">The current page number.</param>
        /// <param name="cancellationToken">The cancellation token to cancel operation.</param>
        /// <returns>
        /// A tuple containing:
        /// - A DataTable with distinct rows for the specified page.
        /// - The total count of distinct rows in the original DataTable.
        /// </returns>
        public static (DataTable, int) ApplyDistinctWithPaging(DataTable dataTable, int pageSize, int pageNumber, CancellationToken cancellationToken = default)
        {

            try
            {
                var allRows = dataTable.AsEnumerable();

                var distinctRows = allRows
                    .GroupBy(row => string.Join("||", row.ItemArray))
                    .Select(g => g.First())
                    .ToList();
                int totalDistinctCount = distinctRows.Count;
                var pagedResult = distinctRows
                                    .Skip((pageNumber - 1) * pageSize)
                                    .Take(pageSize);

                DataTable finalTable = pagedResult.Any() ? pagedResult.CopyToDataTable() : dataTable.Clone();
                return (finalTable, totalDistinctCount);
            }
            catch (OperationCanceledException)
            {
                return (null, 0);
            }
        }

        /// <summary>
        /// Removes blank rows from the provided DataTable.
        /// </summary>
        /// <param name="dataTable">The DataTable from which blank rows need to be removed.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        /// A new DataTable containing only rows with at least one non-null and non-whitespace field.
        /// If no such rows exist, an empty DataTable with the same schema is returned.
        /// </returns>
        public static DataTable RemoveBlankRows(DataTable dataTable, CancellationToken cancellationToken = default)
        {
            try
            {
                var filteredRows = dataTable.AsEnumerable()
                        .Where(row => row.ItemArray.Any(field =>
                            !(field is DBNull) && !string.IsNullOrWhiteSpace(field.ToString())))
                        .ToList();

                return filteredRows.Any() ? filteredRows.CopyToDataTable() : dataTable.Clone();
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        /// <summary>
        /// Reorders the columns of a DataTable based on the display labels provided in group functions,
        /// using column metadata from the provided procedure table definitions.
        /// Also removes columns marked as disabled in the group function list.
        /// </summary>
        /// <param name="originalTable">The original DataTable to reorder.</param>
        /// <param name="groupFunctions">The list of group functions containing display names and flags like Disabled.</param>
        /// <param name="procedureTables">The list of procedure table definitions containing column names and display names.</param>
        /// <param name="organizationLabel"></param>
        /// <returns>
        /// A new DataTable with columns reordered to match the display labels from the group functions
        /// and rows populated accordingly. Disabled columns are removed from the result.
        /// </returns>
        public static DataTable ReorderAndFilterTable(DataTable originalTable, List<GroupFunctionDto> groupFunctions,
                                              List<ProcedureTableDto> procedureTables, string organizationLabel)
        {
            var fieldLabelDictionary = procedureTables
                                       .SelectMany(proc => proc.Columns)
                                       .ToDictionary(x => x.ColumnName, x => x.DisplayName);

            var existingColumnNames = new HashSet<string>(
                                        originalTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName),
                                        StringComparer.OrdinalIgnoreCase);

            foreach (DataColumn col in originalTable.Columns)
            {
                if (fieldLabelDictionary.TryGetValue(col.ColumnName, out string displayName))
                {
                    var matchingGroupFunction = groupFunctions.FirstOrDefault(gf => gf.CustomLabel == displayName);

                    string desiredName = !string.IsNullOrWhiteSpace(matchingGroupFunction?.FieldLabel)
                        ? matchingGroupFunction.FieldLabel
                        : matchingGroupFunction?.CustomLabel ?? displayName;

                    // Rename only if it's a different name and not already used
                    if (!string.IsNullOrWhiteSpace(desiredName)
                        && !string.Equals(col.ColumnName, desiredName, StringComparison.OrdinalIgnoreCase)
                        && !existingColumnNames.Contains(desiredName))
                    {
                        col.ColumnName = desiredName;
                        existingColumnNames.Add(desiredName);
                    }
                }
            }
            for (int i = 0; i < originalTable.Columns.Count; i++)
            {
                var col = originalTable.Columns[i];
                if (col.ColumnName.Contains("Sector", StringComparison.OrdinalIgnoreCase) && i < groupFunctions.Count && groupFunctions[i].FieldLabel != "Sector")
                {
                    col.ColumnName = col.ColumnName.Replace("Sector", organizationLabel, StringComparison.OrdinalIgnoreCase);
                }
            }
            return originalTable;
        }

        /// <summary>
        /// Reorders the columns of a DataTable based on the display labels provided in group functions,
        /// </summary>
        /// <param name="originalTable">The original DataTable to reorder.</param>
        /// <param name="groupFunctions">The list of group functions containing display names and flags like Disabled.</param>
        /// <param name="procedureTables">The list of procedure table definitions containing column names and display names.</param>
        /// <param name="isSpReport">The sp report</param>
        /// <returns></returns>
        public static DataTable ReorderAndFilterTable(DataTable originalTable, List<GroupFunctionDto> groupFunctions,
                                                            List<ProcedureTableDto> procedureTables, bool isSpReport)
        {
            var fieldLabelDictionary = procedureTables
                .SelectMany(proc => proc.Columns)
                .ToDictionary(x => x.ColumnName, x => x.DisplayName);


            var existingColumnNames = new HashSet<string>(
                originalTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName),
                StringComparer.OrdinalIgnoreCase);

            foreach (DataColumn col in originalTable.Columns)
            {
                if (fieldLabelDictionary.TryGetValue(col.ColumnName, out string displayName))
                {
                    var matchingGroupFunction = groupFunctions.FirstOrDefault(gf => gf.CustomLabel == displayName);
                    string desiredName = matchingGroupFunction?.CustomLabel ?? displayName;

                    // Skip renaming if desired name already exists and it's not the same column
                    if (!string.IsNullOrWhiteSpace(desiredName)
                        && !string.Equals(col.ColumnName, desiredName, StringComparison.OrdinalIgnoreCase)
                        && !existingColumnNames.Contains(desiredName))
                    {
                        col.ColumnName = desiredName;
                        existingColumnNames.Add(desiredName);
                    }
                }
            }

            return originalTable;
        }

        /// <summary>
        /// Helper method to replace only the first occurrence of a substring
        /// </summary>
        /// <param name="text"></param>
        /// <param name="search"></param>
        /// <param name="replace"></param>
        /// <param name="comparison"></param>
        /// <returns></returns>
        private static string ReplaceFirst(string text, string search, string replace, StringComparison comparison)
        {
            int pos = text.IndexOf(search, comparison);
            if (pos < 0) return text;
            return text.Substring(0, pos) + replace + text.Substring(pos + search.Length);
        }

        /// <summary>
        /// Serializes the payload for the API call based on the filter request.
        /// </summary>
        private static string GetSerializedPayload(FilterRequestDto filterRequestDto)
        {
            return filterRequestDto.ProcedureId == null
                ? JsonSerializer.Serialize(new
                {
                    adminMode = true,
                    fieldId = filterRequestDto.FieldId,
                    dataFilters = filterRequestDto.DataFilters
                })
                : JsonSerializer.Serialize(new
                {
                    adminMode = true,
                    parameterId = filterRequestDto.FieldId,
                    procId = filterRequestDto.ProcedureId,
                    dataFilters = filterRequestDto.DataFilters
                });
        }

        /// <summary>
        /// Asynchronously retrieves a lookup list based on the specified filters in the <see cref="FilterRequestDto"/>.
        /// </summary>
        /// <param name="filterRequestDto">The request DTO containing filter criteria, such as the field ID and data filters.</param>
        /// <returns>
        /// A <see cref="ValueTask{TResult}"/> containing a <see cref="ResponseBase"/> which holds the result of the lookup list retrieval.
        /// If successful, the response will contain a list of <see cref="GetLookupListDto"/> objects. 
        /// In case of failure, an error response will be returned.
        /// </returns>
        /// <exception cref="JsonException">Thrown when there is an issue with serializing or deserializing the JSON data.</exception>
        /// <exception cref="XrmInternalServerErrorResponse">A general exception handler to catch any unexpected errors during the operation.</exception>

        public async ValueTask<ResponseBase> GetLookupListAsync(FilterRequestDto filterRequestDto)
        {
            try
            {
                var serializedPayload = GetSerializedPayload(filterRequestDto);

                List<DropdownItemsDto> lookupItems = new List<DropdownItemsDto>();
                var settings = XrmReportSettings.GetSettings();
                settings.UserId = "";

                var apiUrl = filterRequestDto.ProcedureId == null ? ReportConstant.UrlGetLookup : ReportConstant.UrlGetPrmLookupList;

                var stringCreateResult = await XrmCallReportApi.ExecuteCallReportApi(apiUrl, serializedPayload, settings);
                var serializedResultDict = JsonSerializer.Deserialize<Dictionary<string, string>>(stringCreateResult);

                string encryptedSql;
                if (serializedResultDict.TryGetValue(ReportConstant.Sql, out encryptedSql))
                {
                    var sql = XrmDotNetReportHelper.Decrypt(encryptedSql);
                    if (filterRequestDto.ProcedureId != null)
                    {
                        string pattern = @"\[(.*?)\]";

                        MatchCollection matches = Regex.Matches(sql, pattern, RegexOptions.None, TimeSpan.FromMilliseconds(100));

                        if (matches.Count >= 2)
                        {
                            Match secondMatch = matches[1];

                            string secondContent = secondMatch.Value;

                            string replacement = secondContent.Insert(1, "Filter");

                            sql = sql.Remove(secondMatch.Index, secondMatch.Length)
                                         .Insert(secondMatch.Index, replacement);
                        }
                    }
                    var sqlDar = await XrmReportDataAccessService.ApplyDataAccessRightsOptimized(sql, _serviceProvider, 0, true);
                    sql = sqlDar.Item1;
                    DBHelper db = new DBHelper();
                    DataSet ds = await db.ExecuteDataSetWithTempTableAsync(sqlDar.Item3 + sql, sqlDar.Item2, null, CommandType.Text);
                    if (ds != null && ds.Tables.Count > 0)
                    {
                        foreach (DataRow row in ds.Tables[0].Rows)
                        {
                            var lookupItem = new DropdownItemsDto
                            {
                                Value = StringOperationExtensions.Encode(row[0].ToString(), StringOperation.CommaToSquareBracket),
                                Text = row[1].ToString()
                            };
                            lookupItems.Add(lookupItem);
                        }
                    }
                }

                List<string> selectedValues = new List<string>();

                if (filterRequestDto.PaginationDto?.UserValues != null &&
                        filterRequestDto.PaginationDto.UserValues.TryGetValue("SelectedValues", out var selectedValuesObj))
                {
                    if (selectedValuesObj is IEnumerable<object> enumerableObj)
                    {
                        selectedValues = enumerableObj.Select(x => x?.ToString()).Where(x => !string.IsNullOrEmpty(x)).ToList();
                    }
                }

                var selectedItems = lookupItems.Where(x => selectedValues.Contains(x.Value)).ToList();

                var remainingItems = lookupItems.Where(x => !selectedValues.Contains(x.Value)).ToList();
                if (!string.IsNullOrWhiteSpace(filterRequestDto.PaginationDto?.SearchText))
                {
                    remainingItems = remainingItems
                        .Where(x => x.Text.Contains(filterRequestDto.PaginationDto.SearchText, StringComparison.OrdinalIgnoreCase))
                        .ToList();
                }

                int pageIndex = filterRequestDto.PaginationDto?.PageIndex ?? 0;
                int pageSize = filterRequestDto.PaginationDto?.PageSize ?? 10;

                var paginatedItems = remainingItems
                    .OrderBy(x => x.Text)
                    .Skip(pageIndex * pageSize)
                    .Take(pageSize)
                    .ToList();

                var finalResult = selectedItems
                                    .Concat(paginatedItems)
                                    .GroupBy(x => new { x.Text, x.Value })
                                    .Select(g => g.First())
                                    .ToList();


                List<dynamic> list = new List<dynamic>();
                list.Add(new
                {
                    count = lookupItems.Count,
                    column = string.Empty,
                    data = finalResult,
                });

                return new XrmSuccessDataResponse<List<dynamic>>(data: list);
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }

        ///<inheritdoc/>
        public async ValueTask<ResponseBase> UpdateExecutionHistoryAsync(string uKey, ReportExecutionUpdateDto updateDto, CancellationToken cancellationToken = default)
        {
            try
            {

                var result = await _xrmRunReportRepository.UpdateExecutionHistoryAsync(updateDto, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                return result;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }

        ///<inheritdoc/>
        public async ValueTask<ResponseBase> SubmitExecutionHistoryAsync(ExecutionHistoryRequestDto request, RunReportApiCallDto callDto, CancellationToken cancellationToken = default)
        {
            try
            {
                var result = await _xrmRunReportRepository.SubmitExecutionHistoryAsync(request, cancellationToken);

                if (request.OutputTypeId == (int)OutputType.Excel || request.OutputTypeId == (int)OutputType.ExcelExpanded
                    || request.OutputTypeId == (int)OutputType.Csv || request.OutputTypeId == (int)OutputType.Pdf
                    || request.OutputTypeId == (int)OutputType.Text)
                {
                    if (result.ReportId != null && result.IsAutoScheduled == false && result.IsUiExport == false)
                    {
                        var baseurl = await _dbContext.RouteDetails.Where(r => r.Id == 1690).Select(r => r.DownstreamScheme + "://" + r.DownstreamHost + "/" + r.Environment).FirstOrDefaultAsync();
                        // var baseurl = XrmReportSettings.GetSettings().ApplicationApiUrl;

                        string[] apiPath = {
                                      ReportConstant.AuthAPIPath,
                                      ReportConstant.RunBgScheduledReportAPIPath
                                      };
                        CommonProducer.ScheduledReportProducerAsync(result.UKey, MQReportType.AutoScheduledReport, baseurl, apiPath);
                    }

                    else
                    {
                        //intended to call async and not to wait for this process to be completed.
                        var xrmReportApiService = _serviceProvider.GetService<IXrmReportApiService>();
                        var updateDto = _mapper.Map<ReportExecutionUpdateDto>(result);

                        var fileResponse = await RunReportFileAsync(result.Id, request.OutputTypeId, request.UKey, updateDto, result.FilterJson,
                            request.SelectedParameters, request.IsUiExport, callDto, cancellationToken, result.SortingJson);

                        cancellationToken.ThrowIfCancellationRequested();

                        if (!fileResponse.Succeeded)
                            return new XrmBadRequestResponse(fileResponse.Message);

                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }
                result.FilePath = null;
                result.JsonPayload = null;

                return new XrmSuccessWithDataResponse(data: result);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }
        ///<inheritdoc/>
        public async ValueTask HandleExecutionHistoryAsync(ExecutionHistoryDto historyDto, CancellationToken cancellationToken = default)
        {
            try
            {
                ExecutionHistoryRequestDto requestDto = new ExecutionHistoryRequestDto
                {
                    UKey = historyDto.UKey,
                    OutputTypeId = historyDto.OutPutTypeId,
                    Sql = historyDto.Sql,
                    FilterJson = historyDto.FilterJson,
                    SelectedParameters = historyDto.SelectedParameters,
                    IsUiExport = historyDto.IsUiExport,
                    IsScheduledReport = historyDto.IsScheduledReport
                };

                var response = await SubmitExecutionHistoryAsync(requestDto, null, cancellationToken);

                if (response is XrmSuccessWithDataResponse xrmResponse)
                {
                    var executionHistory = xrmResponse.Data as ReportExecutionHistory;

                    if (executionHistory != null)
                    {
                        byte[] runSql = string.IsNullOrEmpty(historyDto.Sql) ? executionHistory.RunSql : Encoding.UTF8.GetBytes(historyDto.Sql);
                        var updateDto = CreateUpdateDtoAsync(executionHistory.UKey.ToString(), historyDto.Result, historyDto.StartTime, historyDto.EndTime, runSql, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();
                        await UpdateExecutionHistoryAsync(executionHistory.UKey.ToString(), updateDto, cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }


            }
            catch (OperationCanceledException)
            {
                throw;
            }
        }

        /// <summary>
        /// Creates a <see cref="ReportExecutionUpdateDto"/> for updating the execution history based on the report execution result.
        /// </summary>
        /// <param name="uKey">The unique key associated with the report execution.</param>
        /// <param name="result">The result of the report execution.</param>
        /// <param name="startTime">The time the report execution started.</param>
        /// <param name="endTime">The time the report execution ended.</param>
        /// <param name="runSql">The SQL query used during report execution. Can be null for non-query executions.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>A <see cref="ReportExecutionUpdateDto"/> populated with the necessary update information.</returns>
        private ReportExecutionUpdateDto CreateUpdateDtoAsync(string uKey, ResponseBase result, TimeSpan startTime, TimeSpan endTime, byte[] runSql, CancellationToken cancellationToken = default)
        {
            try
            {
                var updateDto = new ReportExecutionUpdateDto
                {
                    UKey = uKey,
                    FileName = string.Empty,
                    FilePath = string.Empty,
                    ExecutionStartedOn = startTime,
                    Exception = result.StatusCode == 200 ? string.Empty : (result.Message?.Split('$')[0] ?? string.Empty),
                    RunSql = runSql
                };

                if (result.Succeeded)
                {
                    var timeTaken = endTime;
                    updateDto.ExecutionStatusId = (int)ExecutionType.Completed;
                    updateDto.ExecutionCompletedOn = timeTaken;
                    updateDto.JsonPayload = null;
                }
                else
                {
                    updateDto.ExecutionStatusId = (int)ExecutionType.Failed;
                    updateDto.Exception = result.Message?.Split('$')[0] ?? string.Empty;
                    updateDto.ExecutionCompletedOn = endTime;
                }

                return updateDto;
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        // Helper methods
        private List<(string Field, string AggregateFunc)> ProcessGroupFunctions(List<GroupFunctionDto> groupFunctions)
        {
            var firstColumn = groupFunctions
                .Where(x => !x.Disabled && x.GroupFunc != "Only in Detail" && x.GroupFunc != "Group in Detail")
                .FirstOrDefault();

            var filteredGroupFunctionList = groupFunctions.Where(x => !x.GroupInGraph).ToList();
            var pivotIndex = groupFunctions.FindIndex(x => x.GroupFunc == "Pivot");

            if (pivotIndex != -1 && pivotIndex + 1 < groupFunctions.Count)
            {
                groupFunctions.RemoveAt(pivotIndex + 1);
            }
            if (firstColumn != null && !filteredGroupFunctionList.Contains(firstColumn))
            {
                filteredGroupFunctionList.Insert(0, firstColumn);
            }

            return filteredGroupFunctionList.Select(x => (!string.IsNullOrEmpty(x.GroupFunc))
                    ? (Field: $"{x.CustomLabel} ({x.GroupFunc})", AggregateFunc: x.GroupFunc)
                    : (Field: x.CustomLabel, AggregateFunc: string.Empty)).ToList();
        }

        private List<(string Field, string AggregateFunc)> ProcessSelectedFields(List<SelectedFieldDto> selectedFieldDtos)
        {
            var firstColumn = selectedFieldDtos
                .Where(x => !x.Disabled && x.AggregateFunction != "Only in Detail" && x.AggregateFunction != "Group in Detail")
                .FirstOrDefault();

            var pivotIndex = selectedFieldDtos.FindIndex(x => x.AggregateFunction == "Pivot");

            if (pivotIndex != -1 && pivotIndex + 1 < selectedFieldDtos.Count)
            {
                selectedFieldDtos.RemoveAt(pivotIndex + 1);
            }
            var filteredList = selectedFieldDtos.Where(x => !x.GroupInGraph).ToList();

            if (firstColumn != null && !filteredList.Contains(firstColumn))
            {
                filteredList.Insert(0, firstColumn);
            }

            return filteredList.Select(x =>
            {
                var decodedField = StringOperationExtensions.Decode(x.FieldName, StringOperation.IdToSquareBracketHash);
                var aggregateFunc = string.IsNullOrEmpty(x.AggregateFunction) ? string.Empty : x.AggregateFunction;
                return (Field: decodedField, AggregateFunc: aggregateFunc);
            }).ToList();
        }

        private async Task<XrmDotNetReportResultModel> GenerateDefaultReportModelAsync(ReportDataDto request, int? storedProcId, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                List<(string Field, string AggregateFunc)> filteredFieldsWithAgg;

                var filteredGroup = request.GroupFunctions.Select(x => (!string.IsNullOrEmpty(x.GroupFunc))
                    ? (Field: $"{x.CustomLabel} ({x.GroupFunc})", AggregateFunc: x.GroupFunc)
                    : (Field: x.CustomLabel, AggregateFunc: string.Empty)).ToList();

                filteredFieldsWithAgg = filteredGroup;

                var aggregateFunctions = filteredFieldsWithAgg
                                        .Where(x => !string.IsNullOrEmpty(x.AggregateFunc))
                                        .GroupBy(x => Regex.Replace(x.Field, @"\s*\(.*?\)", "").Trim())
                                        .ToDictionary(g => g.Key, g => g.First().AggregateFunc);

                var normalizedTableCols = request.DtPaged.Columns.Cast<DataColumn>()
                                            .Select(c =>
                                            {
                                                var colName = NormalizeColumnName(c.ColumnName);
                                                if (colName.Contains("Sector", StringComparison.OrdinalIgnoreCase))
                                                {
                                                    colName = colName.Replace("Sector", request.GetRunReport.OrganizationLabel, StringComparison.OrdinalIgnoreCase);
                                                }
                                                return colName;
                                            })
                                            .ToList();

                var groupFunctions = request.GroupFunctions?.ToList() ?? new List<GroupFunctionDto>();

                var finalGroupFunctions = new List<GroupFunctionDto>();

                if (!string.IsNullOrEmpty(request.GetRunReport.Data.PivotColumn) && (request.GetRunReport.DrillDownRow == null || !request.GetRunReport.DrillDownRow?.Any() == true))
                {
                    for (int i = 0; i < groupFunctions.Count; i++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var gf = groupFunctions[i];
                        if (gf == null || gf.Disabled)
                            continue;

                        if (string.Equals(gf.GroupFunc, "Pivot", StringComparison.OrdinalIgnoreCase))
                        {
                            if (i + 1 < groupFunctions.Count)
                            {
                                GroupFunctionDto item = null;
                                for (int j = i + 1; j < groupFunctions.Count; j++)
                                {
                                    var next = groupFunctions[j];
                                    if (!(next.Disabled))
                                    {
                                        item = next;
                                        break;
                                    }
                                }
                                if (item == null)
                                {
                                    continue;
                                }

                                var groupFunctionLabels = groupFunctions
                                            .Where(gf => !string.IsNullOrEmpty(gf.CustomLabel))
                                            .Select(gf => NormalizeColumnName(gf.CustomLabel))
                                            .ToHashSet(StringComparer.OrdinalIgnoreCase);



                                // All columns from normalized table
                                var dynamicCols = normalizedTableCols
                                    .Where(c => !groupFunctionLabels.Contains(NormalizeColumnName(c))) // exclude any col already in GroupFunctions
                                    .ToList();

                                if (!string.IsNullOrEmpty(request.GetRunReport.OrganizationLabel) && !storedProcId.HasValue)
                                {
                                    for (int j = 0; j < dynamicCols.Count; j++)
                                    {
                                        var col = dynamicCols[j];
                                        if (col.Contains("Sector", StringComparison.OrdinalIgnoreCase))
                                        {
                                            var newColumnName = col.Replace("Sector", request.GetRunReport.OrganizationLabel, StringComparison.OrdinalIgnoreCase);

                                            var matchingColumns = dynamicCols.Any(c => c.Equals(newColumnName, StringComparison.OrdinalIgnoreCase));
                                            if (!matchingColumns)
                                            {
                                                dynamicCols[j] = newColumnName;
                                            }
                                        }
                                    }
                                }

                                foreach (var col in dynamicCols)
                                {
                                    // Check if already added
                                    if (finalGroupFunctions.Any(x =>
                                            string.Equals(x.CustomLabel, col, StringComparison.OrdinalIgnoreCase)))
                                        continue;

                                    var dynamicGf = new GroupFunctionDto
                                    {
                                        FieldId = item.FieldId,
                                        CustomLabel = col,
                                        DecimalPlaces = item.DecimalPlaces,
                                        GroupFunc = item.GroupFunc,
                                        DataFormat = item.DataFormat,
                                        FieldSettings = item.FieldSettings
                                    };

                                    finalGroupFunctions.Add(dynamicGf);
                                }
                                i++;
                            }

                            continue;
                        }
                        if (!finalGroupFunctions.Any(x =>
                                string.Equals(x.CustomLabel, gf.CustomLabel, StringComparison.OrdinalIgnoreCase)))
                        {
                            finalGroupFunctions.Add(gf);
                        }
                    }
                }
                else
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    // Replace the filteredGroupFunctions assignment with the following code:
                    finalGroupFunctions = request.GroupFunctions?
                        .Where(gf =>
                        {
                            if (gf.Disabled) return false;
                            var field = gf.CustomLabel ?? gf.CustomLabel;
                            var normalizedField = NormalizeColumnName(field);
                            // Use Contains instead of Equals for OrganizationLabel comparison
                            return normalizedTableCols.Any(col => string.Equals(col, normalizedField, StringComparison.OrdinalIgnoreCase))
                                || (!string.IsNullOrEmpty(request.GetRunReport.OrganizationLabel) &&
                                    normalizedField.Contains(request.GetRunReport.OrganizationLabel, StringComparison.OrdinalIgnoreCase)
                                    && !normalizedField.Contains(request.GetRunReport.OrganizationLabel, StringComparison.OrdinalIgnoreCase));
                        })
                        .ToList();
                }
                if (!string.IsNullOrEmpty(request.UKey))
                {
                    foreach (var field in request.SelectedFields)
                    {
                        field.FieldName = StringOperationExtensions.Decode(field.FieldName, StringOperation.IdToSquareBracketHash);
                        if (field.FieldName.Contains("Sector", StringComparison.OrdinalIgnoreCase))
                        {
                            field.FieldName = field.FieldName.Replace("Sector", request.GetRunReport.OrganizationLabel,
                                StringComparison.OrdinalIgnoreCase);
                        }
                    }
                }

                var selectedFields = request.SelectedFields?.ToList() ?? new List<SelectedFieldDto>();
                var finalSelectedFields = new List<SelectedFieldDto>();

                if (string.IsNullOrEmpty(request.UKey))
                {
                    for (int i = 0; i < selectedFields.Count; i++)
                    {
                        selectedFields[i].AggregateFunction = selectedFields[i].SelectedFieldAggregate;
                    }
                }
                if (!string.IsNullOrEmpty(request.GetRunReport.Data.PivotColumn) && (request.GetRunReport.DrillDownRow == null || !request.GetRunReport.DrillDownRow?.Any() == true))
                {
                    for (int i = 0; i < selectedFields.Count; i++)
                    {
                        var sf = selectedFields[i];
                        if (sf == null || sf.Disabled)
                            continue;

                        // Detect Pivot
                        if (string.Equals(sf.AggregateFunction, "Pivot", StringComparison.OrdinalIgnoreCase))
                        {
                            if (i + 1 < selectedFields.Count)
                            {
                                SelectedFieldDto item = null;
                                for (int j = i + 1; j < selectedFields.Count; j++)
                                {
                                    var next = selectedFields[j];
                                    if (!(next.Disabled))
                                    {
                                        item = next;
                                        break;
                                    }
                                }
                                if (item == null)
                                {
                                    continue;
                                }

                                // Find dynamic pivot columns
                                var selectedFieldLabels = selectedFields
                                                        .Where(gf => !string.IsNullOrEmpty(gf.FieldName))
                                                        .Select(gf => NormalizeColumnName(gf.FieldName))
                                                        .ToHashSet(StringComparer.OrdinalIgnoreCase);

                                // All columns from normalized table
                                var dynamicCols = normalizedTableCols
                                    .Where(c => !selectedFieldLabels.Contains(NormalizeColumnName(c))) // exclude any col already in GroupFunctions
                                    .ToList();

                                if (!string.IsNullOrEmpty(request.GetRunReport.OrganizationLabel) && !storedProcId.HasValue)
                                {
                                    for (int j = 0; j < dynamicCols.Count; j++)
                                    {
                                        var col = dynamicCols[j];
                                        if (col.Contains("Sector", StringComparison.OrdinalIgnoreCase))
                                        {
                                            var newColumnName = col.Replace("Sector", request.GetRunReport.OrganizationLabel, StringComparison.OrdinalIgnoreCase);

                                            var matchingColumns = dynamicCols.Any(c => c.Equals(newColumnName, StringComparison.OrdinalIgnoreCase));
                                            if (!matchingColumns)
                                            {
                                                dynamicCols[j] = newColumnName;
                                            }
                                        }
                                    }
                                }

                                foreach (var col in dynamicCols)
                                {
                                    // Prevent duplicates
                                    if (finalSelectedFields.Any(x =>
                                            string.Equals(x.FieldName, col, StringComparison.OrdinalIgnoreCase)))
                                        continue;

                                    var dynamicSf = new SelectedFieldDto
                                    {
                                        FieldId = item.FieldId,
                                        FieldName = col,
                                        AggregateFunction = item.AggregateFunction,
                                        DecimalPlaces = item.DecimalPlaces,
                                        DataFormat = item.DataFormat,
                                        FieldSettings = item.FieldSettings,
                                        FieldType = item.FieldType
                                    };

                                    finalSelectedFields.Add(dynamicSf);
                                }

                                // Skip raw item
                                i++;
                            }

                            continue; // skip Pivot itself
                        }

                        // Normal handling
                        if (!finalSelectedFields.Any(x =>
                                string.Equals(x.FieldName, sf.FieldName, StringComparison.OrdinalIgnoreCase)))
                        {
                            finalSelectedFields.Add(sf);
                        }
                    }
                }
                else
                {
                    finalSelectedFields = request.SelectedFields?
                        .Where(sf =>
                        {
                            if (sf.Disabled) return false;
                            var field = StringOperationExtensions.Decode(sf.FieldName, StringOperation.IdToSquareBracketHash)
                                        ?? StringOperationExtensions.Decode(sf.FieldName, StringOperation.IdToSquareBracketHash);
                            var normalizedField = NormalizeColumnName(field);

                            return normalizedTableCols.Any(col =>
                                       string.Equals(col, normalizedField, StringComparison.OrdinalIgnoreCase));
                        })
                        .ToList();
                }

                var currencyCode = await _repository.GetCurrencyCodeAsync();
                if (string.IsNullOrEmpty(request.UKey) && request.GetRunReport.DrillDownRow.Count > 0)
                {
                    for (int i = 0; i < finalSelectedFields.Count; i++)
                    {
                        finalSelectedFields[i].FieldSettings = request.GroupFunctions[i].FieldSettings;
                    }
                }

                return new XrmDotNetReportResultModel
                {
                    ReportData = XrmDotNetReportHelper.DataTableToDotNetReportDataModel(request.DtPaged, request.SqlFields, aggregateFunctions,
                    request.IsSubTotal, request.IncludeSubTotal, finalGroupFunctions, finalSelectedFields, currencyCode.CurrencySymbol, storedProcId, request.GetRunReport),
                    ReportSql = request.Sql,
                    Pager = new XrmDotNetReportPagerModel
                    {
                        CurrentPage = request.PageNumber,
                        PageSize = request.PageSize,
                        TotalRecords = request.TotalRecords,
                        TotalPages = (int)(request.TotalRecords == request.PageSize ? (request.TotalRecords / request.PageSize) : (request.TotalRecords / request.PageSize) + 1)
                    }
                };
            }
            catch (OperationCanceledException)
            {

                return null;
            }
        }
        /// <summary>
        /// Retrieves filtered report data based on the provided parameters and formats it into a result model.
        /// </summary>
        /// <param name="request">The type of report (e.g., Line, Pie, Bar) to determine how to format the output.</param>  
        /// <param name="storedProcId">The ID of the stored procedure to execute.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        /// A <see cref="XrmDotNetReportResultModel"/> containing the filtered report data, warnings, SQL query, and pagination information.
        /// </returns>
        private async ValueTask<XrmDotNetReportResultModel> GetFilteredReportDataAsync(ReportDataDto request, int? storedProcId, CancellationToken cancellationToken = default)
        {
            try
            {
                XrmDotNetReportResultModel model;
                if ((string.Equals(request.VisualizationType, nameof(OutputType.Line), StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(request.VisualizationType, nameof(OutputType.Pie), StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(request.VisualizationType, nameof(OutputType.Bar), StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(request.VisualizationType, ReportConstant.bar, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(request.VisualizationType, nameof(OutputType.Combo), StringComparison.OrdinalIgnoreCase)))
                {
                    var selectedFieldDtos = System.Text.Json.JsonSerializer.Deserialize<List<SelectedFieldDto>>(
                                        System.Text.Json.JsonSerializer.Serialize(request.SelectedFields));
                    //if (string.IsNullOrEmpty(request.UKey))
                    //{
                    foreach (var group in request.GroupFunctions)
                    {
                        if (group.CustomLabel.Contains(request.GetRunReport.OrganizationLabel))
                        {
                            group.CustomLabel = group.CustomLabel.Replace(request.GetRunReport.OrganizationLabel, "Sector", StringComparison.OrdinalIgnoreCase);
                        }
                    }
                    //}

                    var groupFunctionDtos = System.Text.Json.JsonSerializer.Deserialize<List<GroupFunctionDto>>(
                                        System.Text.Json.JsonSerializer.Serialize(request.GroupFunctions));
                    List<(string Field, string AggregateFunc)> filteredFieldsWithAgg;

                    filteredFieldsWithAgg = string.IsNullOrEmpty(request.UKey)
                                            ? ProcessGroupFunctions(request.GroupFunctions)
                                            : ProcessSelectedFields(request.SelectedFields);

                    var filteredFields = filteredFieldsWithAgg.Select(x => x.Field).ToList();

                    var aggregateFunctions = filteredFieldsWithAgg
                                            .Where(x => !string.IsNullOrEmpty(x.AggregateFunc))
                                            .ToDictionary(x => Regex.Replace(x.Field, @"\s*\(.*?\)", "").Trim(), x => x.AggregateFunc);


                    filteredFields = filteredFields
                                        .Select(field => Regex.Replace(field, @"\s*\(.*?\)", ""))
                                        .ToList();

                    // 1. Unify the list of fields based on UKey (same as before)
                    var fieldDefs = string.IsNullOrEmpty(request.UKey)
                        ? groupFunctionDtos.Cast<dynamic>().ToList()
                        : selectedFieldDtos.Cast<dynamic>().ToList();

                    // 2. Find the index of the Pivot field
                    var pivotIndex = string.IsNullOrEmpty(request.UKey)
                        ? fieldDefs.FindIndex(x => x.GroupFunc == "Pivot")
                        : fieldDefs.FindIndex(x => x.AggregateFunction == "Pivot");

                    bool includePivotColumns = false;

                    if (pivotIndex != -1)
                    {
                        for (int i = pivotIndex + 1; i < fieldDefs.Count; i++)
                        {
                            var nextField = fieldDefs[i];
                            if (nextField.Disabled == true)
                            {
                                continue;
                            }

                            if (nextField.GroupInGraph == false)
                            {
                                includePivotColumns = true;
                            }
                            else
                            {
                                includePivotColumns = false;
                            }
                            break;
                        }
                    }

                    var customFieldAliases = selectedFieldDtos
                                            .Where(x => x.FieldId == 0)
                                            .Select(x =>
                                            {
                                                // Custom fields might use CustomLabel or FieldName as the alias source
                                                string rawName = !string.IsNullOrEmpty(x.CustomLabel) ? x.CustomLabel : x.FieldName;

                                                // Apply standard decoding
                                                string decoded = StringOperationExtensions.Decode(rawName, StringOperation.IdToSquareBracketHash);

                                                // Apply the same normalization regex you use in the loop
                                                string normalized = Regex.Replace(decoded, @"(%\sof\s|\s+)", " ").Trim();
                                                normalized = Regex.Replace(normalized, @"\s*\(.*?\)$", "").Trim();
                                                return normalized.Trim('[', ']');
                                            })
                                            .ToHashSet(StringComparer.OrdinalIgnoreCase);

                    var matchedSqlFields = request.SqlFields
                    .Where(sqlField =>
                    {
                        var lastAsIndex = sqlField.LastIndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
                        string alias = lastAsIndex >= 0
                            ? sqlField.Substring(lastAsIndex + 4).Trim().Trim('[', ']')
                            : sqlField.Trim().Trim('[', ']');

                        var normalizedAlias = Regex.Replace(alias, @"(%\sof\s|\s+)", " ").Trim();
                        normalizedAlias = Regex.Replace(normalizedAlias, @"\s*\(.*?\)$", "").Trim();
                        normalizedAlias = normalizedAlias.Trim('[', ']');

                        bool isCustomField = customFieldAliases.Contains(normalizedAlias);

                        var isPivotGenerated = Regex.IsMatch(sqlField.TrimStart(), @"^__\s+AS\s+", RegexOptions.IgnoreCase)
                                    && !isCustomField
                                    && !filteredFields.Any(f => f.Equals(normalizedAlias, StringComparison.OrdinalIgnoreCase));

                        // If it's truly a Pivot column, use the pivot flag
                        if (isPivotGenerated)
                        {
                            return includePivotColumns;
                        }

                        return filteredFields.Any(field =>
                        {
                            var normalizedField = Regex.Replace(field, @"(%\sof\s|\s+)", " ").Trim();
                            normalizedField = Regex.Replace(normalizedField, @"\s*\(.*?\)$", "").Trim();
                            normalizedField = normalizedField.Trim('[', ']');

                            return normalizedAlias.Equals(normalizedField, StringComparison.OrdinalIgnoreCase);
                        });
                    })
                    .ToList();


                    var dataTable = new DataTable();

                    var columnNameMapping = new Dictionary<string, string>(); // Original -> Renamed

                    var columnNames = matchedSqlFields
                                        .Select(sqlField =>
                                        {
                                            var lastAsIndex = sqlField.LastIndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
                                            string alias = lastAsIndex >= 0
                                                ? sqlField.Substring(lastAsIndex + 4).Trim().Trim('[', ']')
                                                : sqlField.Trim().Trim('[', ']');

                                            var match = Regex.Match(sqlField, @"AS\s+(?:\[(.*?)\]|(\S.*))", RegexOptions.IgnoreCase);
                                            if (!match.Success) return null;

                                            var columnName = alias;   // match.Groups[1].Success ? match.Groups[1].Value.Trim() : match.Groups[2].Value.Trim();
                                            columnName = columnName.Trim('[', ']'); // Removes square brackets if present

                                            var baseColumnName = columnName.Trim(); //Regex.Replace(columnName, @"\s*\([^)]*\)$", "").Trim();

                                            return new { OriginalName = columnName, BaseName = baseColumnName };

                                        })
                                        .Where(alias => alias != null)
                                        .ToList();

                    var columnNameCounts = new Dictionary<string, int>();

                    foreach (var column in columnNames)
                    {
                        string baseColumnName = column.BaseName;
                        string uniqueColumnName = baseColumnName;

                        // Handle duplicates by appending numbers
                        if (dataTable.Columns.Contains(uniqueColumnName))
                        {
                            if (!columnNameCounts.ContainsKey(baseColumnName))
                                columnNameCounts[baseColumnName] = 1;

                            columnNameCounts[baseColumnName]++;
                            uniqueColumnName = $"{baseColumnName} {columnNameCounts[baseColumnName]}";
                        }
                        else
                        {
                            columnNameCounts[baseColumnName] = 0; // Initialize count for the first occurrence
                        }

                        // 🔍 Infer original data type from request.Dtpaged
                        var originalColumn = request.DtPaged.Columns
                                                .Cast<DataColumn>()
                                                .FirstOrDefault(col =>
                                                    col.ColumnName.Equals(column.OriginalName, StringComparison.OrdinalIgnoreCase));

                        var columnDataType = originalColumn?.DataType ?? typeof(string); // Default to string if not found

                        // ✅ Create column with correct data type
                        dataTable.Columns.Add(uniqueColumnName, columnDataType);

                        // Store mapping
                        columnNameMapping[column.OriginalName] = uniqueColumnName;
                    }

                    string Normalize(string input)
                    {
                        return Regex.Replace(input ?? "", @"\s+", " ").Trim().ToLowerInvariant();
                    }

                    var dataTableColumnLookup = request.DtPaged.Columns
                                                    .Cast<DataColumn>()
                                                    .ToDictionary(
                                                        col => Normalize(col.ColumnName),
                                                        col => col.ColumnName,
                                                        StringComparer.OrdinalIgnoreCase
                                                    );

                    // Populate rows using the mapping
                    foreach (DataRow row in request.DtPaged.Rows)
                    {
                        var newRow = dataTable.NewRow();
                        foreach (var mapping in columnNameMapping)
                        {
                            string originalName = mapping.Key;
                            string renamedName = mapping.Value;

                            string normalizedOriginal = Normalize(originalName);

                            if (dataTableColumnLookup.TryGetValue(normalizedOriginal, out var actualColName))
                            {
                                newRow[renamedName] = row[actualColName];
                            }
                            else
                            {
                                newRow[renamedName] = DBNull.Value;
                            }
                        }
                        dataTable.Rows.Add(newRow);
                    }

                    var currencyCode = await _repository.GetCurrencyCodeAsync();

                    var normalizedTableCols = dataTable.Columns.Cast<DataColumn>()
                                                       .Select(c => NormalizeColumnName(c.ColumnName))
                                                       .ToList();

                    var selectedFields = selectedFieldDtos?.ToList() ?? new List<SelectedFieldDto>();
                    //selectedFields = selectedFields.Where(x => !x.HideInDetail).ToList();
                    var finalSelectedFields = new List<SelectedFieldDto>();

                    if (!string.IsNullOrEmpty(request.GetRunReport.Data.PivotColumn) && (request.GetRunReport.DrillDownRow == null || !request.GetRunReport.DrillDownRow?.Any() == true))
                    {
                        for (int i = 0; i < selectedFields.Count; i++)
                        {
                            var sf = selectedFields[i];
                            if (sf == null || sf.Disabled)
                                continue;

                            // Detect Pivot
                            if (string.Equals(sf.AggregateFunction, "Pivot", StringComparison.OrdinalIgnoreCase))
                            {
                                if (i + 1 < selectedFields.Count)
                                {
                                    SelectedFieldDto item = null;
                                    for (int j = i + 1; j < selectedFields.Count; j++)
                                    {
                                        var next = selectedFields[j];
                                        if (!(next.Disabled))
                                        {
                                            item = next;
                                            break;
                                        }
                                    }
                                    if (item == null)
                                    {
                                        continue;
                                    }

                                    // Find dynamic pivot columns
                                    var selectedFieldLabels = selectedFields
                                                            .Where(gf => !string.IsNullOrEmpty(gf.FieldName))
                                                            .Select(gf => NormalizeColumnName(gf.FieldName))
                                                            .ToHashSet(StringComparer.OrdinalIgnoreCase);

                                    // All columns from normalized table
                                    var dynamicCols = normalizedTableCols
                                        .Where(c => !selectedFieldLabels.Contains(NormalizeColumnName(c))) // exclude any col already in GroupFunctions
                                        .ToList();

                                    if (!string.IsNullOrEmpty(request.GetRunReport.OrganizationLabel) && !storedProcId.HasValue)
                                    {
                                        for (int j = 0; j < dynamicCols.Count; j++)
                                        {
                                            var col = dynamicCols[j];
                                            if (col.Contains("Sector", StringComparison.OrdinalIgnoreCase))
                                            {
                                                var newColumnName = col.Replace("Sector", request.GetRunReport.OrganizationLabel, StringComparison.OrdinalIgnoreCase);

                                                var matchingColumns = dynamicCols.Any(c => c.Equals(newColumnName, StringComparison.OrdinalIgnoreCase));
                                                if (!matchingColumns)
                                                {
                                                    dynamicCols[j] = newColumnName;
                                                }
                                            }
                                        }
                                    }

                                    foreach (var col in dynamicCols)
                                    {
                                        // Prevent duplicates
                                        if (finalSelectedFields.Any(x =>
                                                string.Equals(x.FieldName, col, StringComparison.OrdinalIgnoreCase)))
                                            continue;

                                        var dynamicSf = new SelectedFieldDto
                                        {
                                            FieldId = item.FieldId,
                                            FieldName = col,
                                            AggregateFunction = item.AggregateFunction,
                                            DecimalPlaces = item.DecimalPlaces,
                                            DataFormat = item.DataFormat,
                                            FieldType = item.FieldType
                                        };

                                        finalSelectedFields.Add(dynamicSf);
                                    }
                                    // Skip raw item
                                    i++;
                                }

                                continue; // skip Pivot itself
                            }

                            if (!finalSelectedFields.Any(x =>
                                    string.Equals(x.FieldName, sf.FieldName, StringComparison.OrdinalIgnoreCase)))
                            {
                                if (i == 0 || sf.GroupInGraph == false)
                                {
                                    finalSelectedFields.Add(sf);
                                }
                            }
                        }
                    }
                    else
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        finalSelectedFields = request.SelectedFields?
                            .Where(sf =>
                            {
                                if (sf.Disabled) return false;
                                var field = StringOperationExtensions.Decode(sf.FieldName, StringOperation.IdToSquareBracketHash)
                                            ?? StringOperationExtensions.Decode(sf.FieldName, StringOperation.IdToSquareBracketHash);
                                var normalizedField = NormalizeColumnName(field);

                                return normalizedTableCols.Any(col =>
                                           string.Equals(col, normalizedField, StringComparison.OrdinalIgnoreCase));
                            })
                            .ToList();
                    }

                    var groupFunctions = request.GroupFunctions?.ToList() ?? new List<GroupFunctionDto>();
                    groupFunctions = selectedFields
                            .Select(MapToGroupFunction)
                            .Where(gf => gf != null)
                            .ToList();
                    var finalGroupFunctions = new List<GroupFunctionDto>();

                    if (!string.IsNullOrEmpty(request.GetRunReport.Data.PivotColumn) && (request.GetRunReport.DrillDownRow == null || !request.GetRunReport.DrillDownRow?.Any() == true))
                    {
                        for (int i = 0; i < groupFunctions.Count; i++)
                        {
                            var sf = groupFunctions[i];
                            if (sf == null || sf.Disabled)
                                continue;

                            // Detect Pivot
                            if (string.Equals(sf.GroupFunc, "Pivot", StringComparison.OrdinalIgnoreCase))
                            {
                                if (i + 1 < groupFunctions.Count)
                                {
                                    GroupFunctionDto item = null;
                                    for (int j = i + 1; j < selectedFields.Count; j++)
                                    {
                                        var next = groupFunctions[j];
                                        if (!(next.Disabled))
                                        {
                                            item = next;
                                            break;
                                        }
                                    }
                                    if (item == null)
                                    {
                                        continue;
                                    }

                                    // Find dynamic pivot columns
                                    var selectedFieldLabels = groupFunctions
                                                            .Where(gf => !string.IsNullOrEmpty(gf.CustomLabel))
                                                            .Select(gf => NormalizeColumnName(gf.CustomLabel))
                                                            .ToHashSet(StringComparer.OrdinalIgnoreCase);

                                    // All columns from normalized table
                                    var dynamicCols = normalizedTableCols
                                        .Where(c => !selectedFieldLabels.Contains(NormalizeColumnName(c))) // exclude any col already in GroupFunctions
                                        .ToList();

                                    if (!string.IsNullOrEmpty(request.GetRunReport.OrganizationLabel) && !storedProcId.HasValue)
                                    {
                                        for (int j = 0; j < dynamicCols.Count; j++)
                                        {
                                            var col = dynamicCols[j];
                                            if (col.Contains("Sector", StringComparison.OrdinalIgnoreCase))
                                            {
                                                var newColumnName = col.Replace("Sector", request.GetRunReport.OrganizationLabel, StringComparison.OrdinalIgnoreCase);

                                                var matchingColumns = dynamicCols.Any(c => c.Equals(newColumnName, StringComparison.OrdinalIgnoreCase));
                                                if (!matchingColumns)
                                                {
                                                    dynamicCols[j] = newColumnName;
                                                }
                                            }
                                        }
                                    }

                                    foreach (var col in dynamicCols)
                                    {
                                        cancellationToken.ThrowIfCancellationRequested();
                                        // Prevent duplicates
                                        if (finalGroupFunctions.Any(x =>
                                                string.Equals(x.CustomLabel, col, StringComparison.OrdinalIgnoreCase)))
                                            continue;

                                        var dynamicSf = new GroupFunctionDto
                                        {
                                            FieldId = item.FieldId,
                                            CustomLabel = col,
                                            GroupFunc = item.GroupFunc,
                                            DecimalPlaces = item.DecimalPlaces,
                                            DataFormat = item.DataFormat
                                        };

                                        finalGroupFunctions.Add(dynamicSf);
                                    }
                                    // Skip raw item
                                    i++;
                                }

                                continue; // skip Pivot itself
                            }

                            if (!finalGroupFunctions.Any(x =>
                                    string.Equals(x.CustomLabel, sf.CustomLabel, StringComparison.OrdinalIgnoreCase)))
                            {
                                if (i == 0 || sf.GroupInGraph == false)
                                {
                                    finalGroupFunctions.Add(sf);
                                }
                            }
                        }
                    }
                    else
                    {
                        finalGroupFunctions = request.GroupFunctions?
                            .Where(gf =>
                            {
                                if (gf.Disabled) return false;
                                var field = gf.CustomLabel ?? gf.CustomLabel;
                                var normalizedField = NormalizeColumnName(field);
                                return normalizedTableCols.Any(col => string.Equals(col, normalizedField, StringComparison.OrdinalIgnoreCase))
                                    || (!string.IsNullOrEmpty(request.GetRunReport.OrganizationLabel) &&
                                        normalizedField.Contains(request.GetRunReport.OrganizationLabel, StringComparison.OrdinalIgnoreCase)
                                          && !normalizedField.Contains(request.GetRunReport.OrganizationLabel, StringComparison.OrdinalIgnoreCase));
                            })
                            .ToList();
                    }

                    model = new XrmDotNetReportResultModel
                    {
                        ReportData = XrmDotNetReportHelper.DataTableToDotNetReportDataModel(dataTable, matchedSqlFields, aggregateFunctions,
                        request.IsSubTotal, request.IncludeSubTotal, finalGroupFunctions, finalSelectedFields, currencyCode.CurrencySymbol, storedProcId, request.GetRunReport),
                        ReportSql = request.Sql,
                        Pager = new XrmDotNetReportPagerModel
                        {
                            CurrentPage = request.PageNumber,
                            PageSize = request.PageSize,
                            TotalRecords = request.TotalRecords,
                            TotalPages = (int)(request.TotalRecords == request.PageSize ? (request.TotalRecords / request.PageSize) : (request.TotalRecords / request.PageSize) + 1)
                        }
                    };
                }
                else
                {
                    model = await GenerateDefaultReportModelAsync(request, storedProcId, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                }
                model.ReportSettings = request.GetRunReport.ReportSettings;
                await Task.CompletedTask;
                return model;
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        private static GroupFunctionDto MapToGroupFunction(SelectedFieldDto sf)
        {
            if (sf == null) return null;

            return new GroupFunctionDto
            {
                FieldId = sf.FieldId,
                CustomLabel = sf.FieldName,
                GroupFunc = sf.AggregateFunction,
                DecimalPlaces = sf.DecimalPlaces,
                DataFormat = sf.DataFormat,
                Disabled = sf.Disabled,
                HideInDetail = sf.HideInDetail,
                GroupInGraph = sf.GroupInGraph,
                DontSubTotal = sf.DontSubTotal,
                FieldLabel = sf.FieldLabel,
                FieldSettings = sf.FieldSettings
            };
        }

        private string NormalizeColumnName(string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName)) return string.Empty;

            columnName = columnName.Trim('[', ']');

            // Remove prefixes like "Sum of ", "Total of ", etc.
            columnName = Regex.Replace(columnName, @"^[^a-zA-Z0-9]*\s*of\s+", "", RegexOptions.IgnoreCase);

            // Normalize spaces
            return Regex.Replace(columnName, @"\s+", " ").Trim();
        }

        private static string RemoveColumnBySubstring(string sql, string substring)
        {
            var match = Regex.Match(sql, @"(?is)(SELECT\s+)(.*?)(\s+FROM\s)", RegexOptions.Multiline);
            if (!match.Success) return sql;

            string selectPart = match.Groups[1].Value;
            string columnsPart = match.Groups[2].Value;
            string fromPart = match.Groups[3].Value;
            var columns = Regex.Split(columnsPart, @",(?![^\(\[]*[\]\)])"); // naive split, works for most cases

            var filteredColumns = columns
                .Where(col => !col.IndexOf(substring, StringComparison.OrdinalIgnoreCase).Equals(-1) ? false : true)
                .ToArray();

            if (filteredColumns.Length == 0) return sql;
            string newColumnsPart = string.Join(",", filteredColumns);
            return selectPart + newColumnsPart + fromPart + sql.Substring(match.Index + match.Length);
        }
        ///<inheritdoc/>
        public async ValueTask<ResponseBase> RunDashboardReportAsync(RunReportApiCallDto runReportApi)
        {
            try
            {
                string visualizationType = string.Empty;

                if ((runReportApi.OutputTypeId == (int)OutputType.Line || runReportApi.OutputTypeId == (int)OutputType.Pie || runReportApi.OutputTypeId == (int)OutputType.Bar || runReportApi.OutputTypeId == (int)OutputType.Combo) && !string.IsNullOrEmpty(runReportApi.ReportType))
                {
                    visualizationType = ((OutputType)runReportApi.OutputTypeId).ToString();
                    runReportApi.ReportType = visualizationType;
                    ResponseBase result;

                    result = await RunDashboardAsync(runReportApi);

                    if (result is XrmSuccessWithDataResponse successResponse && successResponse.Data is XrmDotNetReportResultModel xrmDotNetReportResultModel)
                    {
                        xrmDotNetReportResultModel.ReportSql = null;
                    }
                    return result;
                }
                else if ((runReportApi.OutputTypeId == (int)OutputType.List || runReportApi.OutputTypeId == (int)OutputType.Summary) || string.IsNullOrEmpty(runReportApi.ReportType))
                {
                    runReportApi.ReportType = visualizationType;
                    ResponseBase result;

                    result = await RunReportApiAsync(runReportApi, false);

                    if (result is XrmSuccessWithDataResponse successResponse && successResponse.Data is XrmDotNetReportResultModel xrmDotNetReportResultModel)
                    {
                        xrmDotNetReportResultModel.ReportSql = null;
                    }
                    return result;
                }
                return new XrmSuccessResponse();
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }

        ///<inheritdoc/>
        public async ValueTask<ResponseBase> ReportDataUpdateAsync(RunReportApiCallDto callDto,
                                                                   CancellationToken cancellationToken = default,
                                                                   bool isEmailTriger = false)
        {
            try
            {
                var xrmReportApiService = _serviceProvider.GetService<IXrmReportApiService>();

                var request = new ExecutionHistoryRequestDto
                {
                    UKey = callDto.UKey,
                    OutputTypeId = (int)OutputType.Text,
                    Sql = null,
                    FilterJson = callDto.SelectedFilters,
                    SelectedParameters = callDto.SelectedParameters,
                    IsUiExport = callDto.IsUiExport,
                    SelectedSorts = callDto.SelectedSorts,
                };

                // Fetch report details
                var reportResponse = await xrmReportApiService.GetReportByUkeyorNameAsync(request.UKey, null);
                if (reportResponse is not XrmSuccessDataResponse<ReportGetDetailsDto> reportSuccess || reportSuccess.Data is null)
                    return new XrmBadRequestResponse(ReportConstant.InvalidReport);

                var reportData = reportSuccess.Data;

                if (!string.Equals(reportData.Json.TableDbName, ReportConstant.GetNexteerInvoiceReport, StringComparison.OrdinalIgnoreCase))
                    return new XrmNotFoundResponse(ReportConstant.NoNexteerReportRequired);

                var executionHistory = await _xrmRunReportRepository.SubmitExecutionHistoryAsync(request, cancellationToken, isEmailTriger);
                var updateDto = _mapper.Map<ReportExecutionUpdateDto>(executionHistory);

                var fileReponse = await GenerateReportFileAsync(executionHistory.Id, request.OutputTypeId, request.UKey, updateDto,
                    executionHistory.FilterJson, request.SelectedParameters, request.IsUiExport, false, callDto, executionHistory.SortingJson,
                    cancellationToken);

                if (!fileReponse.Succeeded)
                    return fileReponse;

                var fileData = ((XrmSuccessDataResponse<FileGenerationResult>)fileReponse).Data;

                if (fileData == null || fileData.ResultSet.Tables[0].Rows.Count <= 0 || string.IsNullOrEmpty(fileData.FileName))
                {
                    return new XrmSuccessResponse(ReportConstant.NoDataAvailable);
                }

                var ftpUploadResult = await UploadNexteerInvoiceFilesAsync(fileData);
                if (!ftpUploadResult.Succeeded)
                    return ftpUploadResult;

                string uploadedFileName = ((XrmSuccessDataResponse<string>)ftpUploadResult).Data;

                var invoiceDetail = await UpdateInvoiceSftpDetailsAsync(fileData);
                await SendNexteerReportEmailAsync(executionHistory.UKey, invoiceDetail.Item1, invoiceDetail.Item2, uploadedFileName);

                return new XrmSuccessResponse(message: ReportConstant.NexteerInvoiceReportMessage);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }

        private async Task<ResponseBase> UploadNexteerInvoiceFilesAsync(FileGenerationResult fileData)
        {
            string sourceFilePath = fileData.FileName;
            string fileName = Path.GetFileName(sourceFilePath);
            string localDirectory = Path.GetDirectoryName(sourceFilePath)!;

            var ftp = await _commonRepository.GetFtpConfigurationAsync(FtpNames.NexteerInvoice.ToString());
            if (ftp == null || ftp.Id <= 0)
                return new XrmBadRequestResponse("FTPConfigNotFound");

            var ftpConfigDto = new FtpConfigurationDto
            {
                Id = ftp.Id,
                ClientDetailId = ftp.ClientDetailId,
                Name = ftp.Name,
                Host = ftp.Host,
                Port = ftp.Port,
                ProtocolType = ftp.ProtocolType,
                AuthType = ftp.AuthType,
                UserName = DecryptIfEncrypted(ftp.UserName),
                Password = DecryptIfEncrypted(ftp.Password),
                PrivateKey = ftp.PrivateKey,
                PassPhrase = DecryptIfEncrypted(ftp.PassPhrase),
                RootPath = string.Empty
            };

            // Step 3️⃣ - Build local and remote file paths
            var now = DateTime.Now;
            string datePart = now.ToString("yyyyMMdd");
            string timePart = now.ToString("HHmmss");

            string mainFileName = fileName.Replace(fileName, $"Nexteer_Acro_{datePart}.txt");
            string archiveFileName = fileName.Replace(fileName, $"Nexteer_Acro_{datePart}_{timePart}.txt");

            string mainLocalFile = Path.Combine(localDirectory, mainFileName);
            string archiveLocalFile = Path.Combine(localDirectory, archiveFileName);

            File.Copy(sourceFilePath, mainLocalFile, overwrite: true);
            File.Copy(sourceFilePath, archiveLocalFile, overwrite: true);

            string mainRemotePath = $"{ftp.RootPath}/{mainFileName}";
            string archiveRemoteFolder = $"{ftp.RootPath}/Archive";
            string archiveRemotePath = $"{archiveRemoteFolder}/{archiveFileName}";

            try
            {
                //await _ftpUploader.DownloadFileAsync(archiveRemoteFolder, sourceFilePath, ftpConfigDto);

                bool mainExists = await _ftpUploader.FileExistsAsync(mainRemotePath, ftpConfigDto);
                if (mainExists)
                {
                    return new XrmValidationFailResponse(ReportConstant.NexteerReportValidationMessage);
                }

                // Step 6️⃣ - Upload main file
                var mainUpload = await _ftpUploader.UploadFileAsync(mainLocalFile, mainRemotePath, ftpConfigDto);
                if (!mainUpload.Succeeded)
                {
                    return new XrmBadRequestResponse(mainUpload.ErrorMessage);
                }

                var archiveUpload = await _ftpUploader.UploadFileAsync(archiveLocalFile, archiveRemotePath, ftpConfigDto);
                if (!archiveUpload.Succeeded)
                {
                    return new XrmBadRequestResponse(archiveUpload.ErrorMessage);
                }

                return new XrmSuccessDataResponse<string>(mainFileName);
            }
            finally
            {
                CleanupLocalFiles(mainLocalFile, archiveLocalFile);
            }
        }

        /// <summary>
        /// Deletes temporary local files safely.
        /// </summary>
        private static void CleanupLocalFiles(params string[] files)
        {
            foreach (var file in files)
            {
                if (File.Exists(file))
                {
                    try { File.Delete(file); } catch { /* ignore cleanup errors */ }
                }
            }
        }

        private async ValueTask<(int, decimal?)> UpdateInvoiceSftpDetailsAsync(FileGenerationResult output)
        {
            var dt = output?.ResultSet?.Tables?[0];
            if (dt == null || dt.Rows.Count == 0)
                return (0, 0);

            int sftpCount = dt.Rows.Count;
            int uploadedBy = _loggedInUserNo;
            DateTime uploadedOn = _getCurrentDateTime;

            // Get all Invoice IDs from ResultSet.Tables[1]
            var invoiceIds = output.ResultSet.Tables[1]
                               .AsEnumerable()
                               .Select(r => Convert.ToInt32(r["InvoiceRecordID"]))
                               .ToList();

            if (invoiceIds.Count == 0)
                return (0, 0);

            decimal? invoiceAmountSum = dt.AsEnumerable()
                                          .Select(r =>
                                          {
                                              var val = r["DocumentAmount"]?.ToString();
                                              return decimal.TryParse(val, out var result) ? result : 0m;
                                          })
                                          .Sum();

            await _dbContext.InvoiceDetails
                .Where(x => invoiceIds.Contains(x.Id))
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(x => x.SftpCount, _ => sftpCount)
                    .SetProperty(x => x.SftpUploadedBy, _ => uploadedBy)
                    .SetProperty(x => x.SftpUploadedOn, _ => uploadedOn)
                );

            return (sftpCount, invoiceAmountSum);
        }

        private async Task SendNexteerReportEmailAsync(Guid runReportUKey, int sftpCount, decimal? invoiceAmount, string uploadedFileName, CancellationToken cancellationToken = default)
        {
            var currencyCode = await _repository.GetCurrencyCodeAsync();

            var userInfo = await _serviceProvider.GetRequiredService<IUserDetailService>().GetUserContactInfoAsync(_loggedInUserNo);
            var userId = ((XrmSuccessDataResponse<UserContactInfoDto>)userInfo).Data;

            var configureClient = _serviceProvider.GetService<IConfigureClientService>();
            var clientName = ((XrmSuccessDataResponse<string>)await configureClient.GetConfigureClientNameAsync()).Data;

            var hostUrl = await DownloadUrlAsync(runReportUKey, (int)OutputType.Text, cancellationToken);
            var Routeukey = new Guid("A589D80D-74A0-4E32-BD28-32A48C9B5360");
            var reportUkey = "download/" + runReportUKey;
            var url = await _reviewLinkGenerator.GenerateReviewLink(hostUrl, reportUkey, Routeukey, 2);

            var runReportDto = new NexteerReportEmailDto
            {
                YearMonth = DateTime.Now.ToString("yyyyMM"),
                CurrencyCode = currencyCode.CurrencyCode,
                QADFileName = uploadedFileName,
                InvoiceAmount = invoiceAmount,
                RecordCount = sftpCount,
                Url = url
            };

            await _emailTemplateConfiguration.SetEmailTemplate((int)XrmEntities.Reports, ActionId, null, runReportDto, userId.UserId, null);
        }
        ///<inheritdoc/>
        public async ValueTask<ResponseBase> GetDarAsync(int xrmEntityId, int userNo)
        {
            try
            {
                var ids = await XrmReportDataAccessService.GetEntityValidRecordIds((XrmEntities)xrmEntityId, _serviceProvider, userNo);
                return new XrmSuccessDataResponse<IEnumerable<int>>(ids);
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message);
            }

        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="runReportUkey"></param>
        /// <param name="timeoutspan"></param>
        /// <returns></returns>
        public async ValueTask<string> GetReportExecutionHistoryByUKeyAsync(Guid runReportUkey, int timeoutspan)
        {
            AutoScheduleClientDetailsDto clientDetail = await _xrmRunReportRepository.GetClientDetailAsync();
            var emailWithUserIdDtos = new List<EmailWithUserIdDto>();
            var listEmailInfoDto = new List<EmailInformationLogDto>();
            var executionHistory = await _xrmRunReportRepository.GetReportExecutionHistoryByUKeyAsync(runReportUkey);
            var emailContentTemplate = GetFailureTemplate();
            var emailReplacements = new Dictionary<string, string>
                {
                    { "[Time]",DateService.GetCurrentDateTime.ToString("hh:mm:ss tt")},
                    { "[Date]", DateService.GetCurrentDateTime.ToString("dd-MM-yyyy")},
                    { "[User]", "System User" },
                    { "[Status]",  "Failed" },
                    { "[Reason]",  "Execution failed due to threshold timeout." },
                    { "[ClientAppUrl]" ,  clientDetail.ClientUrl },
                    { "[ProgramManagerName]", clientDetail.ProgramManagerName },
                    { "[ProgramManagerEmail]",  clientDetail.ProgramManagerEmail },
                    { "[ProgramManagerContact]",  clientDetail.ProgramManagerContact },
                    { "[ClientName]",  clientDetail.ClientName},
                    { "[TimeoutSpan]", timeoutspan.ToString()},
                    { "[RunCode]", executionHistory.RunCode },
                    { "[ExecutedStartedOn]", executionHistory.RequestedOn.ToString("dd-MM-yyyy hh:mm:ss tt")}

                };

            var emailContent = ReplaceDynamicField(emailContentTemplate, emailReplacements);
            var emailBodycontent = GetEmailHtml().Replace("[BodyHtml]", emailContent);
            var emailBody = ReplaceDynamicField(emailBodycontent, emailReplacements);


            emailWithUserIdDtos.Add(new EmailWithUserIdDto
            {
                Email = "xrmnextgensupport@acrocorp.com",
                UserId = string.Empty
            });
            var subject = $"[ClientName] - Report execution timed out";

            var emailsubject = ReplaceDynamicField(subject, emailReplacements);
            var emailInfo = new EmailInformationLogDto
            {
                TemplateId = 0,
                Subject = emailsubject,
                Content = string.Empty,
                BodyByteData = Encoding.UTF8.GetBytes(emailBody),
                Ukey = Guid.NewGuid(),
                To = emailWithUserIdDtos
            };
            listEmailInfoDto.Add(emailInfo);
            var response = await _emailSaveLogRepo.LogEmailsAsync(Guid.NewGuid(), GetEnvironmentValue.GetEnvironmentName(), "xrmsysadmin@acrocorp.com", listEmailInfoDto);

            CommonProducer.EmailProducerAsync(Guid.NewGuid(), false, false);

            return null;
        }
        private string ReplaceDynamicField(string template, Dictionary<string, string> values)
        {
            foreach (var pair in values)
            {
                template = template.Replace(pair.Key, pair.Value);
            }
            return template;
        }

        private string GetFailureTemplate() =>
                 "<tr><td style=\"padding:15px 15px 20px 15px;\">" +

                 "<p>Support Team,  </p>" +
                 "<p>The report exceeded the maximum processing time of [TimeoutSpan] minutes and was automatically cancelled. The operation could not be completed successfully.</p>" +
                 "<p></p>" +
                 "<p><b>Report Execution Details: </b> </p>" +
                 "<p>Run Report Code: [RunCode]</p>" +
                 "<p>Execution Started On: [ExecutedStartedOn] (UTC)</p>" +
                 "<p>Execution Failed/Cancelled On: [Date] [Time] (UTC)</p>" +
                 "<p>Reason for Failure: [Reason]</p>" +
                 "<p>Next Action: Please analyze the reason for the report failure and identify why it is exceeding the threshold timeout.</p>" +
                "</td></tr>";


        private string GetEmailHtml()
        {

            var html = "<!DOCTYPE html><html><head><style>" +
                        "*{margin: 0; padding: 0px; font-family: sans-serif;}" +
                        "</style></head>" +
                        "<body><style>" +
                        "table.inner-table p { font-size: 12px; margin-bottom: 10px; }" +
                        "table.inner-table table { border-collapse: collapse; margin-bottom: 20px; margin-top: 10px; }" +
                        "table.inner-table p { font-size:12px; }" +
                        "table.inner-table table tr th { padding: 8px; margin: 0; border:#d1d1d1 solid 1px !important; font-size: 12px; text-align:center; }" +
                        "table.inner-table table tr td { padding: 8px; margin: 0; border:#d1d1d1 solid 1px !important; font-size: 12px; }" +
                        "</style>" +
                        "<table cellpadding=\"0\" cellspacing=\"0\" width=\"100%\" style=\"border:#d1d1d1 solid 1px\">" +
                        "<tr><td><table class =\"inner-table\" style=\"font-size: 12px; background: #fff;\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\" align=\"center\" border=\"0\">" +

                        "[BodyHtml]" +

                        "</table></td></tr>" +

                        "<tr><td><table bgcolor=\"#eaedfd\" style=\"font-size: 11px; padding:15px 0; margin-top: 15px; line-height: 18px;\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\" align=\"center\" border=\"0\">" +

                        "<tr><td style=\"border:none; text-align: center; margin-top:20px;\">" +

                         "<table width=\"100%\"  style=\"border:none; text-align: center; font-size: 11px;\"><tr><td>" +

                         "<p style=\"margin:0\">You may click on the following link or enter the following URL in your web browser's address box to access the XRM Solutions web site.</p> " +
                         "<p style=\"margin:0\"><a href=\"[ClientAppUrl]\">[ClientAppUrl]</a> </p> " +
                         "<p style=\"margin:0\">For more information regarding this email or accessing the XRM website, please contact [ProgramManagerName] at <a href=\"mailto:[ProgramManagerEmail]\">[ProgramManagerEmail]</a> or <a href=\"tel:[ProgramManagerContact]\">[ProgramManagerContact]</a></p> " +
                         "<p style=\"margin:0; font-weight: bold; text-decoration: underline;\">Please do not reply to this message. This email box is not monitored.</p>" +
                         "<p style=\"margin:0; font-weight: bold;\" > All dates, if any, in this email with the suffix(UTC) are in the UTC time zone.</p>" +
                         "</td></tr></table></td></tr>" +

                        "<tr><td style=\"border-top:#d1d1d1 solid 1px; text-align: center; padding: 5px 0 !important;\">" +

                        "<p style=\"margin:0\">Copyright  &#169; XRM Solutions Inc. All Rights Reserved</p>" +

                        "</td></tr>" +

                        "</table></td></tr>" +

                        "</table></body></html>";
            return html;
        }
    }
}
