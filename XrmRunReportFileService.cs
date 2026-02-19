using APIService.DataAccess.Interfaces.Administration;
using APIService.DataAccess.Repositories.Base;
using APIService.DTOs.Administration;
using APIService.DTOs.Administration.User;
using APIService.EmailService.DTOs.TemplateRecipient;
using APIService.ReportService.XrmDotNetReport.Dtos;
using APIService.ReportService.XrmDotNetReport.Dtos.SaveReport;
using APIService.ReportService.XrmDotNetReport.Enums;
using APIService.ReportService.XrmDotNetReport.Interfaces;
using APIService.ReportService.XrmDotNetReport.Models;
using CommonUtilities;
using CommonUtilities.Constants;
using CommonUtilities.DTOs;
using CommonUtilities.Enums;
using CommonUtilities.Interface;
using CommonUtilities.ResponseTypes;
using CommonUtilities.Services;
using FluentValidation;
using FTPService.Dto;
using IdentityService.Models.UserIdentity;
using MathNet.Numerics;
using Microsoft.EntityFrameworkCore;
using SqlEncryptionService;
using System.Data;
using System.Data.SqlClient;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Web;

namespace APIService.ReportService.XrmDotNetReport.Service
{
    /// <summary>
    /// Provides functionality to run reports and handle related operations within the system.
    /// </summary>
    public partial class XrmRunReportService : AuxiliaryRepository, IXrmRunReportService
    {
        /// <summary>
        /// Generates a report file asynchronously and updates the report execution history.
        /// </summary>
        /// <param name="runReportId">The identifier of the report run.</param>
        /// <param name="outputTypeId">The identifier for the output type (e.g., Excel, PDF).</param>
        /// <param name="reportUkey">The unique key associated with the report.</param>
        /// <param name="executionUpdateDto">Data transfer object containing the execution update details.</param>
        /// <param name="filters">Optional filters to apply to the report data.</param>
        /// <param name="selectedParameters">List of selected parameters applied during the report execution.</param>
        /// <param name="isUiExport">Flag for export</param>
        /// <param name="isScheduledReport">Indicate report is scheduled or not</param>
        /// <param name="callDto">The request DTO containing the request DTO.</param>
        /// <param name="cancellationToken"></param>
        /// <param name="sorting"></param>
        private async ValueTask<ResponseBase> GenerateReportFileAsync(int runReportId, int outputTypeId, string reportUkey,
            ReportExecutionUpdateDto executionUpdateDto, string filters, List<SelectedParameterDto> selectedParameters,
            bool isUiExport, bool isScheduledReport, RunReportApiCallDto callDto, string sorting, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                executionUpdateDto.ExecutionStatusId = (int)ExecutionType.Running;
                await _xrmRunReportRepository.UpdateExecutionHistoryAsync(executionUpdateDto, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                FileGenerationResult result = null;
                try
                {
                    executionUpdateDto.ExecutionStartedOn = DateService.GetCurrentDateTime.TimeOfDay;

                    result = await GenerateReportFileAsync(runReportId, outputTypeId, filters, selectedParameters, isUiExport, isScheduledReport,
                        callDto, cancellationToken, sorting);

                    cancellationToken.ThrowIfCancellationRequested();
                    //if (result.HasError)
                    //    throw result.Exception;
                    if (result.HasError)
                    {
                        ExceptionDispatchInfo.Capture(result.Exception).Throw();
                        throw new InvalidOperationException("Something went wrong.");
                    }
                    byte[] sqlBytes = string.IsNullOrEmpty(result.RunSql) ? null : System.Text.Encoding.UTF8.GetBytes(result.RunSql);
                    executionUpdateDto.ExecutionStatusId = (int)ExecutionType.Completed;
                    executionUpdateDto.FilePath = Path.Combine("Reports", reportUkey);
                    executionUpdateDto.FileName = Path.GetFileName(result.FileName);
                    executionUpdateDto.RunSql = sqlBytes;
                    executionUpdateDto.ExecutionCompletedOn = DateService.GetCurrentDateTime.TimeOfDay;
                    executionUpdateDto.JsonPayload = null;

                    await _xrmRunReportRepository.UpdateExecutionHistoryAsync(executionUpdateDto, cancellationToken);
                    return new XrmSuccessDataResponse<FileGenerationResult>(result);
                }
                catch (OperationCanceledException)
                {
                    executionUpdateDto.ExecutionStatusId = (int)ExecutionType.Failed;
                    executionUpdateDto.Exception = "TimeOut Exception";
                    executionUpdateDto.ExecutionCompletedOn = (DateService.GetCurrentDateTime.TimeOfDay - executionUpdateDto.ExecutionStartedOn);

                    await _xrmRunReportRepository.UpdateExecutionHistoryAsync(executionUpdateDto);
                    throw;
                }
                catch (Exception ex)
                {
                    executionUpdateDto.ExecutionStatusId = (int)ExecutionType.Failed;
                    executionUpdateDto.Exception = ex.ToString();
                    executionUpdateDto.ExecutionCompletedOn = (DateService.GetCurrentDateTime.TimeOfDay - executionUpdateDto.ExecutionStartedOn);

                    await _xrmRunReportRepository.UpdateExecutionHistoryAsync(executionUpdateDto);
                    return new XrmInternalServerErrorResponse(message: ReportConstant.CommonErrorMessage, ex: ex);
                }
            }
            catch (OperationCanceledException)
            {

                throw;
            }
        }

        private async ValueTask<ResponseBase> RunReportFileAsync(int runReportId, int outputTypeId, string reportUkey,
            ReportExecutionUpdateDto executionUpdateDto, string filters, List<SelectedParameterDto> selectedParameters,
            bool isUiExport, RunReportApiCallDto callDto, CancellationToken cancellationToken = default, string sorting = null)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                var result = await GenerateReportFileAsync(runReportId, outputTypeId, reportUkey, executionUpdateDto, filters,
                    selectedParameters, isUiExport, false, callDto, sorting, cancellationToken);

                cancellationToken.ThrowIfCancellationRequested();
                if (!result.Succeeded)
                    return result;

                var fileGenerationResult = ((XrmSuccessDataResponse<FileGenerationResult>)result).Data;

                var reportDetails = await _repository.GetReportNameAsync(reportUkey, null, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();

                await SendRunReportEmail(runReportId, reportUkey, outputTypeId, callDto, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();

                if (!string.IsNullOrEmpty(callDto.UKey) && reportDetails.IsSendToSftp)
                {
                    await HandleSftpReportUploadAsync(runReportId, outputTypeId, reportUkey, fileGenerationResult, reportDetails, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                }
                return new XrmSuccessResponse(message: ReportConstant.EmailSentSuccessfully);
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

        private static string DecryptIfEncrypted(string value)
        {
            if (string.IsNullOrWhiteSpace(value)) return null;
            return AesSqlEncryption.IsEncrypted(value) ? AesSqlEncryption.Decrypt(value) : value;
        }
        private async Task<string> GenerateDynamicFileName(string fileNameTemplate)
        {
            if (string.IsNullOrWhiteSpace(fileNameTemplate))
                return fileNameTemplate;

            var timeZoneConfigDto = await _timeZoneConfigService.GetOffSetMinAsync();
            var now = _getCurrentDateTime;

            var userLocalTime = now.AddMinutes(timeZoneConfigDto.OffSetMinutes);
            return Regex.Replace(fileNameTemplate, @"\[(.*?)\]", match =>
            {
                string token = match.Groups[1].Value;
                token = token
                    .Replace("YYYY", "yyyy")
                    .Replace("YY", "yy")
                    .Replace("MM", "MM")
                    .Replace("DD", "dd")
                    .Replace("HH", "HH")
                    .Replace("mm", "mm")
                    .Replace("SS", "ss");
                token = Regex.Replace(token, @"(yyyy|yy|MM|dd|HH|mm|ss)|([A-Za-z]+)", m =>
                {
                    if (!string.IsNullOrEmpty(m.Groups[1].Value))
                    {
                        return m.Groups[1].Value;
                    }
                    return $"'{m.Groups[2].Value}'";
                });
                return userLocalTime.ToString(token);
            });
        }
        private async ValueTask<ResponseBase> HandleSftpReportUploadAsync(int runReportId, int outputTypeId,
                                              string reportUkey, FileGenerationResult fileGenerationResult, ReportInfoDto reportDetails, CancellationToken cancellationToken = default)
        {
            try
            {
                string sourceFilePath = fileGenerationResult.FileName;
                string fileName = Path.GetFileName(sourceFilePath);
                string localDirectory = Path.GetDirectoryName(sourceFilePath)!;

                var ftp = await _repository.GetFtpInformationAsync(reportDetails.FtpFolderId, cancellationToken);
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
                string mainFileName = fileName.Replace(fileName, await GenerateDynamicFileName(reportDetails.FtpFileName + ".xlsx"));
                string mainLocalFile = Path.Combine(localDirectory, mainFileName);
                File.Copy(sourceFilePath, mainLocalFile, overwrite: true);
                string mainRemotePath = $"{ftp.RootPath}/{mainFileName}";
                //Logic to check already existing file name
                //bool mainExists = await _ftpUploader.FileExistsAsync(mainRemotePath, ftpConfigDto);
                //if (mainExists)
                //{
                //    return new XrmValidationFailResponse(LocalizationConstant.EnitityAlreadyExists);
                //}
                var mainUpload = await _ftpUploader.UploadFileAsync(mainLocalFile, mainRemotePath, ftpConfigDto);
                if (!mainUpload.Succeeded)
                {
                    return new XrmBadRequestResponse(mainUpload.ErrorMessage);
                }

                if (reportDetails.IsSendToSftp)
                {
                    var rptDetails = await _xrmReportCommonRepository.GetReportEmailDetailsAsync(runReportId, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    var emailAddress = GetEmailAddresses(rptDetails.ReportOwnerEmailAddress, rptDetails.FtpEmailRecipients);
                    int actionId = 151;
                    RunReportMailDto runReportDto = new()
                    {
                        ReportName = rptDetails.ReportName,
                        Subject = rptDetails.FtpSubject,
                        SFTPPath = (ftp.ProtocolType.ToLower()) + "://" +
                                   (ftp.Host?.ToLower() ?? string.Empty) +
                                   (string.IsNullOrWhiteSpace(ftp.RootPath) ? string.Empty : "/" + ftp.RootPath.Trim('/').ToLower()),
                        FileName = mainFileName
                    };

                    var msg = await _emailTemplateConfiguration.SetEmailTemplate((int)XrmEntities.Reports, actionId, null, runReportDto, null, emailAddress, null, cancellationToken);
                }

                return new XrmSuccessResponse(message: ReportConstant.EmailSentSuccessfully);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }

        }

        /// <summary>
        /// Sends an email with a download link for the specified report.
        /// </summary>
        /// <param name="runReportId">The identifier of the report run.</param>
        /// <param name="runReportUkey">The unique key associated with the report run.</param>
        /// <param name="outputTypeId">The identifier for the output type (e.g., Excel, PDF).</param>
        /// <param name="callDto">The API call DTO containing additional information.</param>
        /// <param name="cancellationToken"></param>        
        private async ValueTask SendRunReportEmail(int runReportId, string runReportUkey, int outputTypeId, RunReportApiCallDto callDto, CancellationToken cancellationToken = default)
        {
            try
            {
                var apiSettings = XrmReportSettings.GetSettings(cancellationToken);
                double noOfDays = (double)apiSettings.ReportFileDeleteAfterDays;
                var Routeukey = new Guid("A589D80D-74A0-4E32-BD28-32A48C9B5360");
                var executionHistory = _dbContext.ReportExecutionHistories
                    .Where(r => r.Id == runReportId)
                    .FirstOrDefault();
                //Run without save case
                if (executionHistory.ReportId == null && executionHistory.IsAutoScheduled)
                {
                    var hostUrl = await DownloadUrlAsync(executionHistory.UKey, outputTypeId, cancellationToken);
                    var ExecutedByEmailId = await _userDetailRepository.GetUserContactInfoAsync(executionHistory.ExecutedBy);
                    var emailAddress = GetEmailAddresses(ExecutedByEmailId.EmailAddress, null);
                    var reportUkey = "download/" + executionHistory.UKey;
                    var url = await _reviewLinkGenerator.GenerateReviewLink(hostUrl, reportUkey, Routeukey, (int)noOfDays);
                    RunReportMailDto runReportDto = new()
                    {
                        ReportName = callDto.BaseReportXrmEntityName,
                        Url = url,
                        Subject = callDto.BaseReportXrmEntityName + " Report"
                    };
                    int actionId = 118;
                    if (noOfDays > 0)
                    {
                        string formattedDate = DateTime.Now.AddDays(Convert.ToInt32(noOfDays - 1)).ToString("dddd, MMMM dd, yyyy");
                        runReportDto.ReportAccessibleTime = formattedDate;
                        actionId = 113;
                    }
                    var msg = await _emailTemplateConfiguration.SetEmailTemplate((int)XrmEntities.Reports, actionId, null, runReportDto, null, emailAddress, null, cancellationToken);
                    return;
                }
                //Saved Report Case
                else if (executionHistory.ReportId != null)
                {
                    var rptDetails = await _xrmReportCommonRepository.GetReportEmailDetailsAsync(runReportId, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    if (rptDetails.IsAutoScheduled || rptDetails.SendViaEmail)
                    {
                        var hostUrl = await DownloadUrlAsync(rptDetails.RunReportUKey, outputTypeId, cancellationToken);
                        var emailAddress = GetEmailAddresses(rptDetails.ReportOwnerEmailAddress, rptDetails.ReportEmailAddress);
                        var reportUkey = "download/" + rptDetails.RunReportUKey;
                        var url = await _reviewLinkGenerator.GenerateReviewLink(hostUrl, reportUkey, Routeukey, (int)noOfDays);
                        RunReportMailDto runReportDto = new()
                        {
                            ReportName = rptDetails.ReportName,
                            Url = url,
                            Subject = rptDetails.IsAutoScheduled ? rptDetails.ReportName : rptDetails.ReportSubject
                        };
                        int actionId = 118;
                        if (noOfDays > 0)
                        {
                            string formattedDate = DateTime.Now.AddDays(Convert.ToInt32(noOfDays - 1)).ToString("dddd, MMMM dd, yyyy");
                            runReportDto.ReportAccessibleTime = formattedDate;
                            actionId = 113;
                        }
                        var msg = await _emailTemplateConfiguration.SetEmailTemplate((int)XrmEntities.Reports, actionId, null, runReportDto, null, emailAddress, null, cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException)
            {

                throw;
            }
            catch (Exception ex)
            {
                _ = new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }

        /// <summary>
        /// Retrieves and formats email addresses for the specified report.
        /// </summary>
        /// <param name="ownerEmail">The owner's email address.</param>
        /// <param name="emails">Additional email addresses, separated by commas.</param>
        /// <returns>A list of formatted email addresses.</returns>
        private EmailExternalRecipientDto GetEmailAddresses(string ownerEmail, string emails)
        {
            var emailAddresses = CommonMethod.GetEmailAddress(ownerEmail);
            emailAddresses.AddRange(CommonMethod.GetEmailAddress(emails));
            return new EmailExternalRecipientDto
            {
                To = emailAddresses.Distinct(StringComparer.OrdinalIgnoreCase).ToList()
            };
        }

        /// <summary>
        /// Generates a report file based on the specified parameters and output type.
        /// </summary>
        /// <param name="runReportId">The identifier of the report run.</param>
        /// <param name="outputTypeId">The identifier for the output type (e.g., Excel, CSV).</param>
        /// <param name="filters">Optional filters to apply to the report data.</param>
        /// <param name="selectedParameters">List of selected parameters applied during the report execution.</param>
        /// <param name="isUiExport">Flag for export</param>
        /// <param name="isScheduledReport">Indicates whether the report is a scheduled report.</param>
        /// <param name="callDto">The request DTO containing the request DTO.</param>
        /// <param name="cancellationToken"></param>
        /// <param name="sorting"></param>
        /// <returns>A task containing a tuple with file content, file name, and SQL query.</returns>
        private async ValueTask<FileGenerationResult> GenerateReportFileAsync(int runReportId, int outputTypeId, string filters,
            List<SelectedParameterDto> selectedParameters, bool isUiExport, bool isScheduledReport, RunReportApiCallDto callDto,
            CancellationToken cancellationToken = default, string sorting = null)
        {
            try
            {
                var reportId = _dbContext.ReportExecutionHistories.Where(x => x.Id == runReportId).Select(x => x.ReportId).FirstOrDefault();

                if (!isScheduledReport && string.IsNullOrEmpty(callDto.UKey))
                {
                    filters = JsonSerializer.Serialize(callDto.Json.Filters);
                    selectedParameters = callDto.Json.SelectedParameters;

                    var sortByDefault = new List<SelectedSortDto>();

                    sortByDefault.Add(new SelectedSortDto
                    {
                        FieldId = callDto.Json.SortBy,
                        Descending = callDto.Json.SortDesc
                    });

                    sortByDefault.AddRange(callDto.Json.SelectedSorts);
                    sorting = JsonSerializer.Serialize(sortByDefault);
                }

                //else
                //{
                //    selectedParameters = JsonSerializer.Deserialize<List<SelectedParameterDto>>(filters);
                //}

                // For Unsaved reports, if output type is not list, set to ExcelExpanded

                if (string.IsNullOrEmpty(callDto.UKey) && !isScheduledReport)
                {
                    if (!string.Equals(callDto.Json.ReportType.ToString(), OutputType.List.ToString(), StringComparison.OrdinalIgnoreCase))
                    {
                        outputTypeId = (int)OutputType.ExcelExpanded;
                    }
                }
                // For existing reports, check the DB and if not list then ExcelExpanded
                else
                {
                    int? typeId = _dbContext.Reports.Where(x => x.Id == reportId).Select(x => x.VisualizationTypeId).FirstOrDefault();
                    if (typeId != (int)VisualizationType.List && outputTypeId != (int)OutputType.Csv)
                    {
                        outputTypeId = (int)OutputType.ExcelExpanded;
                    }
                }

                FileGenerationResult result = outputTypeId switch
                {
                    (int)OutputType.Excel => await GenerateExcelFileAsync(reportId, runReportId, filters, selectedParameters, isUiExport, isScheduledReport, callDto, sorting, cancellationToken),
                    (int)OutputType.Csv => await GenerateCsvFileAsync(reportId, runReportId, filters, selectedParameters, callDto, sorting, cancellationToken),
                    (int)OutputType.Text => await GenerateTextFileAsync(reportId, runReportId, filters, selectedParameters, callDto, cancellationToken),
                    (int)OutputType.Pdf => await GeneratePdfFileAsync(runReportId, filters, selectedParameters, isUiExport, isScheduledReport, cancellationToken),
                    (int)OutputType.ExcelExpanded => await GenerateExcelExpandedFileAsync(reportId, runReportId, filters, selectedParameters, isUiExport, isScheduledReport, callDto, sorting, cancellationToken),
                    _ => new FileGenerationResult()
                };

                return result;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
        }

        /// <summary>
        /// Retrieves and processes report SQL data for download.
        /// </summary>
        /// <param name="runReportId">The unique identifier of the report run.</param>
        /// <param name="filters">The filters applied to the report data.</param>
        /// <param name="selectedParameters">List of selected parameters applied during the report execution.</param>
        /// <param name="callDto">The request DTO containing the request DTO.</param>
        /// <param name="cancellationToken"></param>
        /// <param name="isScheduledReport">The flag indicating if the report is scheduled.</param>
        /// <param name="sorting">The sorting criteria for the report data.</param>
        /// <returns>A <see cref="DownloadReportResultDto"/> containing processed report details and SQL query.</returns>
        private async ValueTask<DownloadReportResultDto> GetDownloadReportSqlAsync(int runReportId, string filters, bool isScheduledReport,
            List<SelectedParameterDto> selectedParameters, RunReportApiCallDto callDto, string sorting = null, CancellationToken cancellationToken = default)
        {
            try
            {
                List<SelectedFilterDto> filtersList = null;
                if (filters != null)
                {
                    filtersList = JsonSerializer.Deserialize<List<SelectedFilterDto>>(filters);
                }

                List<SelectedSortDto> sortingList = new List<SelectedSortDto>();
                if (sorting != null && sorting != "")
                {
                    sortingList = JsonSerializer.Deserialize<List<SelectedSortDto>>(sorting);
                }

                var settings = XrmReportSettings.GetSettings();

                var configureClient = _serviceProvider.GetService<IConfigureClientRepository>();

                var organizationLabel = await configureClient.GetBasicInfoAsync();

                cancellationToken.ThrowIfCancellationRequested();

                ReportGetDetailsDto reportDetail = new ReportGetDetailsDto();
                
                DotNetReportAddDto reportDto;

                if (isScheduledReport || !string.IsNullOrEmpty(callDto.UKey))
                {
                    reportDetail = await _xrmRunReportRepository.GetRecRunRptDetailsAsync(runReportId);
                    cancellationToken.ThrowIfCancellationRequested();

                    var requestModel = JsonSerializer.Serialize(new
                    {
                        reportId = reportDetail.ReportId,
                        adminMode = true,
                        buildSql = false,
                        SelectedFilters = filtersList
                    });

                    var stringContentResult = await XrmCallReportApi.ExecuteCallReportApi("/ReportApi/LoadReport", requestModel, settings);
                    cancellationToken.ThrowIfCancellationRequested();

                    if (stringContentResult == null)
                    {
                        return null;
                    }

                    var processed = PreprocessJsonDateFieldsAsync(stringContentResult);

                    reportDto = JsonSerializer.Deserialize<DotNetReportAddDto>(processed, new JsonSerializerOptions
                    { PropertyNameCaseInsensitive = true });
                }
                else
                {
                    reportDetail = await _xrmRunReportRepository.GetRecRunRptExecutedByAsync(runReportId);
                    cancellationToken.ThrowIfCancellationRequested();
                    callDto.Json.FolderId = 1; // In case of run the default folder id is 1
                    var serializedJson = JsonSerializer.Serialize(callDto.Json);

                    reportDto = JsonSerializer.Deserialize<DotNetReportAddDto>(serializedJson, new JsonSerializerOptions
                    { PropertyNameCaseInsensitive = true });

                    filtersList = callDto.Json.Filters;

                    var selectedSort = new List<SelectedSortDto>();

                    selectedSort.Add(new SelectedSortDto
                    {
                        FieldId = callDto.Json.SortBy,
                        Descending = callDto.Json.SortDesc
                    });

                    selectedSort.AddRange(callDto.Json.SelectedSorts);
                    sortingList = selectedSort;
                }

                var reportSettings = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(reportDto.ReportSettings);
                bool showExpandOption = false;

                if (reportSettings != null &&
                    reportSettings.TryGetValue("ShowExpandOption", out var element) &&
                    element.ValueKind is JsonValueKind.True or JsonValueKind.False)
                {
                    showExpandOption = element.GetBoolean();
                }

                foreach (var field in reportDto.SelectedFields)
                {
                    field.fieldName = StringOperationExtensions.Decode(field.fieldName, StringOperation.IdToSquareBracketHash);
                    if (field.fieldName.Contains("Sector", StringComparison.OrdinalIgnoreCase))
                    {
                        field.fieldName = field.fieldName.Replace("Sector", organizationLabel.OrganizationLabel,
                            StringComparison.OrdinalIgnoreCase);
                    }
                    field.fieldFormating = field.fieldFormat;
                    field.DecimalPlacesDigit = field.DecimalPlaces;
                    //??
                    field.aggregateFunction = field.aggregateFunction;
                    field.hideInDetail = field.hideInDetail;

                    var matchingGroupFunc = reportDto.GroupFunctionList
                                .FirstOrDefault(gf => gf.CustomLabel.Equals(field.fieldName, StringComparison.OrdinalIgnoreCase)
                                 && !string.IsNullOrWhiteSpace(gf.FieldLabel));

                    if (matchingGroupFunc != null)
                    {
                        field.fieldName = matchingGroupFunc.FieldLabel;
                        field.fieldLabel = matchingGroupFunc.FieldLabel;
                    }
                }
                if (!isScheduledReport && string.IsNullOrEmpty(callDto.UKey))
                {
                    for (int i = 0; i < reportDto.SelectedFields.Count; i++)
                    {
                        reportDto.SelectedFields[i].fieldFormating = reportDto.GroupFunctionList[i].DataFormat;
                        reportDto.SelectedFields[i].aggregateFunction = reportDto.GroupFunctionList[i].GroupFunc;
                        reportDto.SelectedFields[i].hideInDetail = reportDto.GroupFunctionList[i].HideInDetail;
                        reportDto.SelectedFields[i].fieldSettings = reportDto.GroupFunctionList[i].FieldSettings;

                    }
                }
                if (!string.IsNullOrEmpty(callDto.UKey))
                {
                    for (int i = 0; i < reportDto.SelectedFields.Count; i++)
                    {
                        reportDto.GroupFunctionList[i].FieldSettings = reportDto.SelectedFields[i].fieldSettings;
                        reportDto.GroupFunctionList[i].FieldId = reportDto.SelectedFields[i].fieldId;
                        if (reportDto.GroupFunctionList[i].CustomFieldDetails.Any())
                        {
                            reportDto.GroupFunctionList[i].IsCustom = true;
                        }
                    }
                }

                var options = new JsonSerializerOptions
                {
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    WriteIndented = true
                };

                reportDto.Filters = filtersList;

                if (sortingList.Count > 0)
                {
                    reportDto.SortBy = sortingList[0].FieldId;
                    reportDto.SortDesc = sortingList[0].Descending;
                    reportDto.SelectedSorts = sortingList.Skip(1).ToList();
                }
                else
                {
                    reportDto.SelectedSorts = reportDto.SelectedSorts;
                    reportDto.SortBy = reportDto.SortBy;
                    reportDto.SortDesc = reportDto.SortDesc;
                }

                reportDto.SelectedParameters = selectedParameters;
                reportDto.DrillDownRow = new List<ReportDataRowItemModelDto>();

                string reportJson;
                reportJson = JsonSerializer.Serialize(reportDto, options);

                var response = JsonSerializer.Serialize(new
                {
                    SaveReport = false,
                    ReportJson = reportJson
                }, options);

                settings.UserId = string.Empty;
                settings.ClientId = string.Empty;

                var stringCreateResult = await XrmCallReportApi.ExecuteCallReportApi("/ReportApi/RunReport", response, settings);

                var reportResult = JsonSerializer.Deserialize<DownloadReportResultDto>(stringCreateResult, new JsonSerializerOptions
                { PropertyNameCaseInsensitive = true });

                reportResult.PivotColumn = PreparePivotData(reportDto.GroupFunctionList).PivotColumn;
                reportResult.PivotFunction = PreparePivotData(reportDto.GroupFunctionList).PivotFunction;

                int onlyTop = reportDto.OnlyTop ?? 0;

                var reportSql = reportResult.Sql;
                reportSql = HttpUtility.HtmlDecode(reportSql);
                reportSql = XrmDotNetReportHelper.Decrypt(reportSql);

                string replacement = "[rpt].[$1]";
                string pattern = @"(?<=\bFrom\s+)(?!\[rpt\]\.)\[([^\]]+)\]";
                reportSql = Regex.Replace(reportSql, pattern, replacement, RegexOptions.IgnoreCase, TimeSpan.FromMilliseconds(100));
                reportSql = reportSql.Replace("GetDate()", "CAST(GETDATE() As Date)", StringComparison.OrdinalIgnoreCase);
                reportSql = StringTransformer.DecodeMultiple(reportSql, new[] { StringOperation.IdToSquareBracketHash, StringOperation.CommaToSquareBracket });
                reportSql = reportSql.Replace("{UserGroupId}", _loggedInUserGroupId.ToString());
                reportSql = RemoveColumnBySubstring(reportSql, "__prm__");

                // Adjust DateTime columns for user's timezone
                var (columnIdentifiers, fullyQualifiedDateTimeColumns) = await BuildDateTimeColumnInfoAsync();

                var offsetTime = UserInfo.GetOffsetMinutes();
                var rawSql = reportSql;

                reportSql = ApplyTimeZoneOffsetToSql(reportSql, fullyQualifiedDateTimeColumns, offsetTime);

                List<int?> matchGroup;

                if (reportDto.DrillDownRow?.Any() == true)
                {
                    matchGroup = reportDto.GroupFunctionList
                        .Where(x => !x.Disabled && !x.HideInDetail)
                        .Select(x => x.FieldId)
                        .ToList();
                }
                else
                {
                    if (!string.IsNullOrEmpty(reportResult.PivotColumn))
                    {
                        var pivotIndex = reportDto.GroupFunctionList.FindIndex(x => x.GroupFunc == "Pivot");

                        matchGroup = reportDto.GroupFunctionList
                                                      .Where((x, i) =>
                                                          !x.Disabled &&
                                                          x.GroupFunc != "Only in Detail" &&
                                                          x.GroupFunc != "Group in Detail" &&
                                                          i != pivotIndex && i != pivotIndex + 1
                                                      )
                                                      .Select(x => x.FieldId)
                                                      .ToList();
                    }
                    else
                    {
                        // Normal logic: Exclude 'Only in Detail', 'Group in Detail', Disabled fields
                        matchGroup = reportDto.GroupFunctionList
                            .Where(x => !x.Disabled && x.GroupFunc != "Only in Detail" && x.GroupFunc != "Group in Detail")
                            .Select(x => x.FieldId)
                            .ToList();
                    }

                }

                var selectedFieldIds = reportDto.SelectedFieldIDs
                                                        .Where(id => matchGroup.Contains(id))
                                                        .ToList();
                string orderByClause = string.Empty;
                var sortByDefault = new List<SelectedSortDto>();

                if (!reportSql.StartsWith("EXEC"))
                {
                    if (matchGroup.Contains((int)reportDto.SortBy))
                    {
                        sortByDefault.Add(new SelectedSortDto
                        {
                            FieldId = reportDto.SortBy,
                            Descending = reportDto.SortDesc
                        });
                    }

                    sortByDefault.AddRange(reportDto.SelectedSorts
                        .Where(s => matchGroup.Contains(s.FieldId)));

                    orderByClause = await GetOrderByAsync(sortByDefault, selectedFieldIds);

                    int orderByIndex = reportSql.IndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase);
                    if (orderByIndex != -1)
                    {
                        string afterOrderBy = reportSql.Substring(orderByIndex + "ORDER BY".Length).Trim();
                        // If ORDER BY has only DESC/ASC, remove it
                        if (afterOrderBy.Equals("DESC", StringComparison.OrdinalIgnoreCase) ||
                            afterOrderBy.Equals("ASC", StringComparison.OrdinalIgnoreCase))
                        {
                            reportSql = reportSql.Substring(0, orderByIndex).Trim();
                        }

                        else if (!string.IsNullOrEmpty(orderByClause))
                        {
                            // Replace existing ORDER BY with custom one
                            string beforeOrderBy = reportSql.Substring(0, orderByIndex);
                            reportSql = $"{beforeOrderBy} {orderByClause}";
                        }
                    }
                    else if (!reportSql.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase))
                    {
                        reportSql = $"{reportSql.Trim()} {(string.IsNullOrEmpty(orderByClause) ? "ORDER BY 1" : orderByClause)}";
                    }
                }

                // Only add OFFSET/FETCH if not a pivot report (i.e., no PivotColumn)
                if (reportSql.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase)
                    && !reportSql.Contains(" TOP ", StringComparison.OrdinalIgnoreCase)
                && string.IsNullOrEmpty(reportResult?.PivotColumn))
                {
                    int pageSize = int.MaxValue;
                    int pageNumber = 1;
                    int offset = (pageNumber - 1) * pageSize;
                    reportSql = reportSql + $" OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY";
                }

                var selectedSorts = new List<SelectedSortDto>();

                if (!reportSql.StartsWith("EXEC"))
                {
                    selectedSorts.Add(new SelectedSortDto
                    {
                        FieldId = reportDto.SortBy,
                        Descending = reportDto.SortDesc
                    });

                    selectedSorts.AddRange(reportDto.SelectedSorts);
                }                                

                reportSql = await ApplyChartGroupingByDate(reportSql, reportDto.ReportType, reportDto.GroupFunctionList, sortByDefault, reportDto.DrillDownRow, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                if (reportDto.StoredProcId.HasValue)
                {
                    var spDar = await XrmReportDataAccessService.ApplyDataAccessRightsForSP(reportSql, _serviceProvider, reportDetail.ExecutedBy, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    reportSql = ModifySqlWithJsonParamsAsync(reportSql, 0, int.MaxValue, false, spDar, selectedParameters, reportDetail.ExecutedBy, reportDto.DrillDownRow, offsetTime, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    reportResult.SqlWithParameters = (reportSql, spDar, string.Empty);
                }
                else
                {
                    reportResult.SqlWithParameters = await XrmReportDataAccessService.ApplyDataAccessRightsOptimized(reportSql, _serviceProvider, reportDetail.ExecutedBy, false, !isScheduledReport && string.IsNullOrEmpty(callDto.UKey) ? callDto.BaseReportXrmEntityId : reportDetail.BaseReportXrmEntityId, cancellationToken);
                }
                cancellationToken.ThrowIfCancellationRequested();
                reportResult.EntityId = !isScheduledReport && string.IsNullOrEmpty(callDto.UKey) ? callDto.BaseReportXrmEntityId : reportDetail.BaseReportXrmEntityId;
                reportResult.LoggedInGroupId = _loggedInUserGroupId;
                reportResult.UserNo = reportDetail.ExecutedBy;
                reportResult.ServiceProvider = _serviceProvider;
                reportResult.ReportUkey = !isScheduledReport && string.IsNullOrEmpty(callDto.UKey) ? callDto.UKey : reportDetail.UKey;
                reportResult.ReportName = !isScheduledReport && string.IsNullOrEmpty(callDto.UKey) ? callDto.BaseReportXrmEntityName?.Replace(" ", "_") : reportDetail.ReportName?.Replace(" ", "_");
                reportResult.ExpandSqls = reportJson;
                reportResult.OnlyTop = onlyTop;

                reportResult.groupFunctions = reportDto.GroupFunctionList
                                                       .Where(x => !x.Disabled && x.GroupFunc != "Only in Detail" && x.GroupFunc != "Group in Detail")
                                                       .ToList();

                reportResult.expandedGroupFunctions = reportDto.GroupFunctionList.ToList();

                reportResult.ColumnDetail = JsonSerializer.Serialize(reportDto.SelectedFields, options);
                reportResult.ReportType = reportDto.ReportType;
                reportResult.StoredProcId = reportDto.StoredProcId;
                reportResult.SelectedFieldIds = selectedFieldIds ?? new List<int?>();
                reportResult.SelectedSorts = selectedSorts;
                reportResult.IncludeSubTotals = reportDto.IncludeSubTotals;
                reportResult.ExecutedBy = reportDetail.ExecutedBy;
                reportResult.OrderByClause = orderByClause;
                reportResult.SqlQuery = reportSql;
                reportResult.ColumnIdentifiers = columnIdentifiers;
                reportResult.FullyQualifiedDateTimeColumns = fullyQualifiedDateTimeColumns;
                reportResult.ShowExpandOption = showExpandOption;
                reportResult.RawSql = rawSql;
                return reportResult;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _ = new XrmInternalServerErrorResponse(ex.Message, ex);
                return new DownloadReportResultDto();
            }
        }
        
        /// <summary>
        /// Generates an Excel file for the specified report execution record.
        /// </summary>
        /// <param name="reportId"> The unique identifier of the report.</param>
        /// <param name="runReportId">The unique identifier of the report run.</param>
        /// <param name="filters">The filters applied to the report data.</param>
        /// <param name="selectedParameters">List of selected parameters applied during the report execution.</param>
        /// <param name="isUiExport">Flag for export</param>
        /// <param name="isScheduledReport">Indicates whether the report is a scheduled report.</param>
        /// <param name="callDto">it is the reuest dto</param>
        /// <param name="sorting">The sorting criteria for the report data.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>A tuple containing the Excel file as a byte array, the file path, and SQL query.</returns>
        private async ValueTask<FileGenerationResult> GenerateExcelFileAsync(int? reportId, int runReportId, string filters, List<SelectedParameterDto> selectedParameters, bool isUiExport, bool isScheduledReport, RunReportApiCallDto callDto, string sorting, CancellationToken cancellationToken = default)
        {
            try
            {
                if (!isScheduledReport && string.IsNullOrEmpty(callDto.UKey))
                {
                    filters = JsonSerializer.Serialize(callDto.Json.Filters);
                    var sortByDefault = new List<SelectedSortDto>();

                    sortByDefault.Add(new SelectedSortDto
                    {
                        FieldId = callDto.Json.SortBy,
                        Descending = callDto.Json.SortDesc
                    });

                    sortByDefault.AddRange(callDto.Json.SelectedSorts);

                    sorting = JsonSerializer.Serialize(sortByDefault);
                }

                List<ApproverDetailDto> levels = new List<ApproverDetailDto>();

                UserDetailPreferenceGetDto userDetailPreference = new();

                cancellationToken.ThrowIfCancellationRequested();

                var reportDetail = await GetDownloadReportSqlAsync(runReportId, filters, isScheduledReport, selectedParameters, callDto, sorting, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();

                bool allExpanded = false;
                bool includeSubtotal = false;

                if (reportDetail.IncludeSubTotals)
                {
                    includeSubtotal = true;
                }

                bool pivot = false;
                var _uploadDirectory = _serviceProvider.GetService<IDocumentAppSettingService>();
                var columns = reportDetail.ColumnDetail == null ? new List<XrmReportHeaderColumn>() : Newtonsoft.Json.JsonConvert.DeserializeObject<List<XrmReportHeaderColumn>>(HttpUtility.UrlDecode(reportDetail.ColumnDetail));

                string chartData = await GetChartDataAsync(reportDetail, (int)OutputType.Excel, filters, selectedParameters, isUiExport, isScheduledReport, callDto,sorting, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                //string chartData = "";

                var userDetails = await _userDetailRepository.GetUserPreferenceByUserAsync(reportDetail.ExecutedBy, null, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();

                var currencyCode = await _repository.GetCurrencyCodeAsync();
                cancellationToken.ThrowIfCancellationRequested();

                var configureClient = _serviceProvider.GetService<IConfigureClientRepository>();
                var organizationLabel = await configureClient.GetBasicInfoAsync();
                cancellationToken.ThrowIfCancellationRequested();

                var excel = await XrmDotNetReportHelper.GetExcelFile(currencyCode.CurrencySymbol, reportDetail.SqlWithParameters, reportDetail.ConnectKey, HttpUtility.UrlDecode(reportDetail.ReportName),
                                                                allExpanded, HttpUtility.UrlDecode(reportDetail.ExpandSqls), columns, includeSubtotal, pivot,
                                                                chartData, reportDetail.groupFunctions, reportDetail.StoredProcId, userDetails.DateFormat,
                                                                null, null, reportDetail.PivotColumn, reportDetail.PivotFunction, organizationLabel.OrganizationLabel,
                                                                reportDetail.OnlyTop, reportDetail.SelectedFieldIds, reportDetail.SelectedSorts, reportDetail.OrderByClause, reportDetail.SqlQuery,
                                                                reportDetail.LoggedInGroupId, null, reportDetail.ColumnIdentifiers, reportDetail.FullyQualifiedDateTimeColumns, reportDetail.RawSql, cancellationToken);

                cancellationToken.ThrowIfCancellationRequested();
                var fileName = (reportDetail.ReportName + ".xlsx");
                var directory = Path.Combine(_uploadDirectory.ReportLibraryPath, reportDetail.ReportUkey);
                var filePath = await CreateFileAsync(excel.fileBytes, fileName, directory);
                cancellationToken.ThrowIfCancellationRequested();
                return new FileGenerationResult
                {
                    FileContent = excel.fileBytes,
                    FileName = filePath,
                    RunSql = excel.combinedSql
                };
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _ = new XrmInternalServerErrorResponse(ex.Message);
                return new FileGenerationResult { Exception = ex };
            }
        }


        /// <summary>
        /// Generates an Excel Expanded file for the specified report execution record.
        /// </summary>
        /// <param name="reportId"> The unique identifier of the report.</param>
        /// <param name="runReportId">The unique identifier of the report run.</param>
        /// <param name="filters">The filters applied to the report data.</param>
        /// <param name="selectedParameters">List of selected parameters applied during the report execution.</param>
        /// <param name="isUiExport">Flag for export</param>
        /// <param name="isScheduledReport">Indicates whether the report is a scheduled report.</param>
        /// <param name="callDto">it is the request dto</param>
        /// <param name="cancellationToken"></param>
        /// <param name="sorting"></param>
        /// <returns>A tuple containing the Excel file as a byte array, the file path, and SQL query.</returns>
        private async ValueTask<FileGenerationResult> GenerateExcelExpandedFileAsync(int? reportId, int runReportId, string filters, List<SelectedParameterDto> selectedParameters, bool isUiExport,
            bool isScheduledReport, RunReportApiCallDto callDto, string sorting = null, CancellationToken cancellationToken = default)
        {
            try
            {
                List<ApproverDetailDto> levels = new List<ApproverDetailDto>();
                UserDetailPreferenceGetDto userDetailPreference = new();
                var reportDetail = await GetDownloadReportSqlAsync(runReportId, filters, isScheduledReport, selectedParameters, callDto, sorting);
                bool allExpanded = !reportDetail.ShowExpandOption;
                bool includeSubtotal = false;

                if (reportDetail.IncludeSubTotals)
                {
                    includeSubtotal = true;
                }

                bool pivot = false;
                var _uploadDirectory = _serviceProvider.GetService<IDocumentAppSettingService>();
                var columns = reportDetail.ColumnDetail == null ? new List<XrmReportHeaderColumn>() : Newtonsoft.Json.JsonConvert.DeserializeObject<List<XrmReportHeaderColumn>>(HttpUtility.UrlDecode(reportDetail.ColumnDetail));

                var onlyAndGroupInFieldIds = reportDetail.groupFunctions
                    .Where(x => x.GroupFunc == "Only in Detail" || x.GroupFunc == "Group in Detail")
                    .Select(c => c.FieldId)
                    .Where(fieldId => fieldId.HasValue)
                    .Select(fieldId => fieldId.Value)
                    .ToList();

                string chartData = await GetChartDataAsync(reportDetail, (int)OutputType.ExcelExpanded, filters, selectedParameters, isUiExport, isScheduledReport, callDto, sorting);
                //string chartData = "";
                var userDetails = await _userDetailRepository.GetUserPreferenceByUserAsync(reportDetail.ExecutedBy, null);
                var currencyCode = await _repository.GetCurrencyCodeAsync();

                var configureClient = _serviceProvider.GetService<IConfigureClientRepository>();
                var organizationLabel = await configureClient.GetBasicInfoAsync();

                var expandedExcel = new ExpandedExcelDar
                {
                    EntityId = reportDetail.EntityId,
                    LoggedInGroupId = reportDetail.LoggedInGroupId,
                    UserNo = reportDetail.UserNo,
                    ServiceProvider = reportDetail.ServiceProvider,
                };

                var excel = await XrmDotNetReportHelper.GetExcelFile(currencyCode.CurrencySymbol, reportDetail.SqlWithParameters, reportDetail.ConnectKey, HttpUtility.UrlDecode(reportDetail.ReportName),
                                                allExpanded, HttpUtility.UrlDecode(reportDetail.ExpandSqls), columns, includeSubtotal, pivot,
                                                chartData, reportDetail.groupFunctions, reportDetail.StoredProcId, userDetails.DateFormat, onlyAndGroupInFieldIds, expandedExcel,
                                                reportDetail.PivotColumn, reportDetail.PivotFunction, organizationLabel.OrganizationLabel, reportDetail.OnlyTop,
                                                reportDetail.SelectedFieldIds, reportDetail.SelectedSorts, reportDetail.OrderByClause,
                                                reportDetail.SqlQuery, reportDetail.LoggedInGroupId, reportDetail.expandedGroupFunctions,
                                                reportDetail.ColumnIdentifiers, reportDetail.FullyQualifiedDateTimeColumns, reportDetail.RawSql, cancellationToken);

                var fileName = (reportDetail.ReportName + ".xlsx");
                var directory = Path.Combine(_uploadDirectory.ReportLibraryPath, reportDetail.ReportUkey);
                var filePath = await CreateFileAsync(excel.fileBytes, fileName, directory);
                return new FileGenerationResult
                {
                    FileContent = excel.fileBytes,
                    FileName = filePath,
                    RunSql = excel.combinedSql
                };

            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _ = new Exception(ex.Message);
                return new FileGenerationResult { Exception = ex }; // ✅ return error result
            }
        }

        /// <summary>
        /// Creates a file asynchronously in the specified directory path.
        /// </summary>
        /// <param name="fileByte">The file content as a byte array.</param>
        /// <param name="directoryPath" > The directory path where the file will be created.</param>        
        /// <param name="fileName">The name of the file to create.</param>
        /// <returns>The file path where the file was created.</returns>
        private async ValueTask<string> CreateFileAsync(byte[] fileByte, string fileName, string directoryPath)
        {
            try
            {
                if (!Directory.Exists(directoryPath))
                {
                    Directory.CreateDirectory(directoryPath);
                }

                string fileExtension = Path.GetExtension(fileName);
                string file = Path.GetFileNameWithoutExtension(fileName);
                if (!string.IsNullOrEmpty(fileExtension))
                {
                    file = file + DateTime.Now.ToString("_MMddyy_HHmmss") + fileExtension;
                }
                string filePath = Path.Combine(directoryPath, file);
                await _fileManagerService.WriteBytesAsync(fileByte, filePath);

                return filePath;
            }
            catch (UnauthorizedAccessException ex)
            {
                _ = new XrmInternalServerErrorResponse("Permission denied: " + ex.Message, ex);
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Generates a CSV file for the specified report execution record.
        /// </summary>
        /// <param name="reportId"> The unique identifier of the report.</param>
        /// <param name="runReportId">The unique identifier of the report run.</param>
        /// <param name="filters">Filters applied to the report data.</param>
        /// <param name="selectedParameters">List of selected parameters applied during the report execution.</param> 
        /// <param name="callDto">it is the request dto</param>
        /// <param name="sorting"></param>
        /// <param name="cancellationToken"></param>        
        /// <returns>A tuple containing the CSV file content as a byte array, the file path, and the SQL query used to generate the report.</returns>
        public async ValueTask<FileGenerationResult> GenerateCsvFileAsync(int? reportId, int runReportId, string filters, List<SelectedParameterDto> selectedParameters,
            RunReportApiCallDto callDto, string sorting, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var reportDetail = await GetDownloadReportSqlAsync(runReportId, filters, false, selectedParameters, callDto, sorting, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                var _uploadDirectory = _serviceProvider.GetService<IDocumentAppSettingService>();
                bool includeSubtotal = false;
                if (reportDetail.IncludeSubTotals)
                {
                    includeSubtotal = true;
                }
                var columns = reportDetail.ColumnDetail == null ? new List<XrmReportHeaderColumn>() : Newtonsoft.Json.JsonConvert.DeserializeObject<List<XrmReportHeaderColumn>>(HttpUtility.UrlDecode(reportDetail.ColumnDetail));

                var userDetails = await _userDetailRepository.GetUserPreferenceByUserAsync(reportDetail.ExecutedBy, null);

                var currencyCode = await _repository.GetCurrencyCodeAsync();

                var configureClient = _serviceProvider.GetService<IConfigureClientRepository>();
                var organizationLabel = await configureClient.GetBasicInfoAsync();


                var csv = await XrmDotNetReportHelper.GetCsvFile(currencyCode.CurrencySymbol, reportDetail.SqlWithParameters, reportDetail.ConnectKey,
                    columns, includeSubtotal, HttpUtility.UrlDecode(reportDetail.ExpandSqls), reportDetail.StoredProcId, reportDetail.groupFunctions, userDetails.DateFormat,
                    reportDetail.PivotColumn, reportDetail.PivotFunction, organizationLabel.OrganizationLabel, reportDetail.OnlyTop,
                    reportDetail.SelectedFieldIds, reportDetail.SelectedSorts, reportDetail.OrderByClause, reportDetail.SqlQuery,
                    reportDetail.LoggedInGroupId, reportDetail.ColumnIdentifiers, reportDetail.FullyQualifiedDateTimeColumns, true, true, cancellationToken: cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                var fileName = (reportDetail.ReportName + ".csv");
                var directory = Path.Combine(_uploadDirectory.ReportLibraryPath, reportDetail.ReportUkey);
                var filePath = await CreateFileAsync(csv.Item1, fileName, directory);

                return new FileGenerationResult
                {
                    FileContent = csv.Item1,
                    FileName = filePath,
                    RunSql = reportDetail.SqlWithParameters.Item1,
                    ResultSet = csv.Item2
                };
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _ = new XrmInternalServerErrorResponse(ex.Message, ex);
                return new FileGenerationResult { Exception = ex }; // ✅ return error result
            }
        }

        /// <summary>
        /// Generates a Text file for the specified report execution record.
        /// </summary>
        /// <param name="reportId"> The unique identifier of the report.</param>
        /// <param name="runReportId">The unique identifier of the report run.</param>
        /// <param name="filters">Filters applied to the report data.</param>
        /// <param name="selectedParameters">List of selected parameters applied during the report execution.</param> 
        /// <param name="callDto">it is the request dto</param>
        /// <param name="cancellationToken"></param>        
        /// <returns>A tuple containing the Text file content as a byte array, the file path, and the SQL query used to generate the report.</returns>
        public async ValueTask<FileGenerationResult> GenerateTextFileAsync(int? reportId, int runReportId, string filters, List<SelectedParameterDto> selectedParameters, RunReportApiCallDto callDto, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var reportDetail = await GetDownloadReportSqlAsync(runReportId, filters, false, selectedParameters, callDto);
                var _uploadDirectory = _serviceProvider.GetService<IDocumentAppSettingService>();
                bool includeSubtotal = false;
                if (reportDetail.IncludeSubTotals)
                {
                    includeSubtotal = true;
                }
                var columns = reportDetail.ColumnDetail == null ? new List<XrmReportHeaderColumn>() : Newtonsoft.Json.JsonConvert.DeserializeObject<List<XrmReportHeaderColumn>>(HttpUtility.UrlDecode(reportDetail.ColumnDetail));

                var userDetails = await _userDetailRepository.GetUserPreferenceByUserAsync(reportDetail.ExecutedBy, null);

                var currencyCode = await _repository.GetCurrencyCodeAsync();

                var configureClient = _serviceProvider.GetService<IConfigureClientRepository>();
                var organizationLabel = await configureClient.GetBasicInfoAsync();

                var txt = await XrmDotNetReportHelper.GetTextFile(currencyCode.CurrencySymbol, reportDetail.SqlWithParameters, reportDetail.ConnectKey,
                    columns, includeSubtotal, HttpUtility.UrlDecode(reportDetail.ExpandSqls), reportDetail.StoredProcId, reportDetail.groupFunctions, userDetails.DateFormat,
                    reportDetail.PivotColumn, reportDetail.PivotFunction, organizationLabel.OrganizationLabel, reportDetail.OnlyTop,
                    reportDetail.SelectedFieldIds, reportDetail.SelectedSorts, reportDetail.OrderByClause, reportDetail.SqlQuery,
                    reportDetail.LoggedInGroupId, false, cancellationToken: cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                var fileName = (reportDetail.ReportName + ".txt");
                var directory = Path.Combine(_uploadDirectory.ReportLibraryPath, reportDetail.ReportUkey);
                var filePath = await CreateFileAsync(txt.Item1, fileName, directory);

                return new FileGenerationResult
                {
                    FileContent = txt.Item1,
                    FileName = filePath,
                    RunSql = reportDetail.SqlWithParameters.Item1,
                    ResultSet = txt.Item2
                };
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _ = new XrmInternalServerErrorResponse(ex.Message, ex);
                return new FileGenerationResult { Exception = ex }; // ✅ return error result
            }
        }

        /// <summary>
        /// Constructs a download URL for the generated report based on its unique key and output type.
        /// </summary>
        /// <param name="uKey">The unique key identifier for the report.</param>
        /// <param name="outputTypeId">The output type ID indicating the report format.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>The constructed download URL as a string, or an empty string if the output type is unsupported.</returns>
        private async Task<string> DownloadUrlAsync(Guid uKey, int outputTypeId, CancellationToken cancellationToken = default)
        {
            try
            {
                var hosturl = await _xrmReportCommonRepository.GetRouteDetailByIdAsync(cancellationToken);

                //var constructedUrl = $"{XrmReportSettings.GetSettings().ApplicationApiUrl.TrimEnd('/')}{routeDetails.UpstreamPathTemplate}"
                //                        .Replace("{version}", "1").Replace("{uKey}", uKey.ToString());

                return (outputTypeId == (int)OutputType.Excel || outputTypeId == (int)OutputType.Text || outputTypeId == (int)OutputType.Csv || outputTypeId == (int)OutputType.Pdf || outputTypeId == (int)OutputType.ExcelExpanded) ? hosturl : string.Empty;
            }
            catch (OperationCanceledException)
            {

                return ReportConstant.ReportTimeOutMessage;
            }
        }

        /// <summary>
        /// Downloads the report file associated with the specified unique key.
        /// </summary>
        /// <param name="uKey">The unique key identifier of the report file.</param>
        /// <returns>A tuple containing the file content as a byte array and the file name, or null if the file is not found.</returns>
        public async ValueTask<Tuple<byte[], string>> DownloadFileAsync(string uKey)
        {
            try
            {
                var executionHistoryRecord = await _dbContext.ReportExecutionHistories
                    .FirstOrDefaultAsync(eh => eh.UKey.ToString() == uKey && eh.ExecutionStatusId != (int)ExecutionType.Deleted);
                if (executionHistoryRecord != null)
                {
                    var _uploadDirectory = _serviceProvider.GetService<IDocumentAppSettingService>();
                    string filePath = Path.Combine(_uploadDirectory.DocumentBaseFolderPath, executionHistoryRecord.FilePath, executionHistoryRecord.FileName);
                    byte[] fileBytes = await File.ReadAllBytesAsync(filePath);
                    return new Tuple<byte[], string>(fileBytes, executionHistoryRecord.FileName);
                }
                return new Tuple<byte[], string>(null, null);
            }
            catch (Exception ex)
            {
                _ = new XrmInternalServerErrorResponse(ex.Message);
                return new Tuple<byte[], string>(null, null);
            }
        }

        /// <summary>
        /// Retrieves the report execution history associated with a unique key.
        /// </summary>
        /// <param name="dto">The unique key identifier of the report execution history.</param>
        /// <returns>A response containing the execution history details or an error response if retrieval fails.</returns>
        public async ValueTask<ResponseBase> GetExeHistoryByUKeyAsync(PaginationandSearchingDto dto = null)
        {
            try
            {
                var report = await _xrmRunReportRepository.GetExeHistoryByUKeyAsync(dto);
                var result = await _paginationRepository.GetDataAsync<ReportExeHistoryGetAllDto>(XrmEntities.Reports, report, dto);
                return result;
            }
            catch (Exception ex)
            {
                return new XrmInternalServerErrorResponse(ex.Message, ex);
            }
        }

        /// <summary>
        /// Deletes records from the ReportExecutionHistories table that are older than one day.
        /// </summary>
        public async ValueTask<ResponseBase> RemoveFileHistoryAsync()
        {
            try
            {
                var apiSettings = XrmReportSettings.GetSettings();
                var documentService = _serviceProvider.GetService<IDocumentAppSettingService>();
                double noOfDays = (double)apiSettings.ReportFileDeleteAfterDays;
                if (noOfDays > 0)
                {
                    DateTime deletionThreshold = DateTime.UtcNow.AddDays(-noOfDays);
                    var oldRecords = await _dbContext.ReportExecutionHistories
                    .Where(record => DateOnly.FromDateTime(record.RequestedOn) <= DateOnly.FromDateTime(deletionThreshold) &&
                                  (record.OutPutTypeId == (int)OutputType.Excel ||
                                   record.OutPutTypeId == (int)OutputType.Csv)).ToListAsync();
                    if (!oldRecords.Any())
                    {
                        Console.WriteLine("No expired records found for deletion.");
                        return new XrmSuccessResponse();
                    }

                    var idsToDelete = new List<int>();
                    var filePaths = new List<string>();

                    foreach (var record in oldRecords)
                    {
                        idsToDelete.Add(record.Id);
                        if (!string.IsNullOrEmpty(record.FilePath) && !string.IsNullOrEmpty(record.FileName))
                        {
                            string fullFilePath = Path.Combine(documentService.DocumentBaseFolderPath, record.FilePath, record.FileName);
                            filePaths.Add(fullFilePath);
                        }
                    }
                    foreach (var filePath in filePaths)
                    {
                        if (System.IO.File.Exists(filePath))
                        {
                            try
                            {
                                System.IO.File.Delete(filePath);
                                string directoryPath = Path.GetDirectoryName(filePath);

                                if (directoryPath != null && Directory.Exists(directoryPath) && !Directory.EnumerateFileSystemEntries(directoryPath).Any())
                                {
                                    Directory.Delete(directoryPath);
                                }
                            }
                            catch (Exception fileEx)
                            {
                                Console.WriteLine($"Error deleting file: {filePath}, Exception: {fileEx.Message}");
                                return new XrmInternalServerErrorResponse(fileEx.Message);
                            }
                        }
                    }
                    if (idsToDelete.Any())
                    {
                        await _dbContext.ReportExecutionHistories
                               .Where(record => idsToDelete.Contains(record.Id))
                               .ExecuteUpdateAsync(setters => setters.SetProperty(r => r.ExecutionStatusId, (int)ExecutionType.Deleted));
                        Console.WriteLine($"{oldRecords.Count} expired records have been deleted successfully.");
                        return new XrmSuccessResponse();
                    }
                }
                return new XrmSuccessResponse();
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred while deleting expired records: " + ex.Message);
                return new XrmInternalServerErrorResponse(ex.Message);
            }
        }

        /// <summary>
        /// Generates a PDF file for the specified report execution record.
        /// </summary>
        /// <param name="runReportId">The unique identifier of the report run.</param>
        /// <param name="filters">Filters applied to the report data.</param>
        /// <param name="selectedParameters">List of selected parameters applied during the report execution.</param>
        /// <param name="isUiExport">Flag for export</param>
        /// <param name="isScheduledReport">Indicates whether the report is a scheduled report.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>A tuple containing the PDF file content as a byte array, the file path, and the SQL query used to generate the report.</returns>
        public async ValueTask<FileGenerationResult> GeneratePdfFileAsync(int runReportId, string filters, List<SelectedParameterDto> selectedParameters, bool isUiExport, bool isScheduledReport, CancellationToken cancellationToken = default)
        {
            try
            {
                var reportDetail = await GetDownloadReportSqlAsync(runReportId, filters, false, selectedParameters, null);
                var _uploadDirectory = _serviceProvider.GetService<IDocumentAppSettingService>();
                bool includeSubtotal = false;
                if (reportDetail.IncludeSubTotals)
                {
                    includeSubtotal = true;
                }
                
                string chartData = await GetChartDataAsync(reportDetail, (int)OutputType.Pdf, filters, selectedParameters, isUiExport, isScheduledReport, null, null);
                var columns = reportDetail.ColumnDetail == null ? new List<XrmReportHeaderColumn>() : Newtonsoft.Json.JsonConvert.DeserializeObject<List<XrmReportHeaderColumn>>(HttpUtility.UrlDecode(reportDetail.ColumnDetail));

                var pdf = await XrmDotNetReportHelper.GetPdfFileAlt(reportDetail.SqlWithParameters, reportDetail.ConnectKey, HttpUtility.UrlDecode(reportDetail.ReportName),
                    chartData, columns, includeSubtotal, reportDetail.StoredProcId, reportDetail.groupFunctions);

                var fileName = (reportDetail.ReportName + ".pdf");
                var directory = Path.Combine(_uploadDirectory.ReportLibraryPath, reportDetail.ReportUkey);
                var filePath = await CreateFileAsync(pdf, fileName, directory);

                return new FileGenerationResult
                {
                    FileContent = pdf,
                    FileName = filePath,
                    RunSql = reportDetail.SqlWithParameters.Item1
                };
            }
            catch (Exception ex)
            {
                _ = new XrmInternalServerErrorResponse(ex.Message, ex);
                return new FileGenerationResult { Exception = ex }; // ✅ return error result
            }
        }

        private static int GetOutputTypeId(string reportType)
        {
            var reportTypeMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
                                        {
                                            { "line", (int)OutputType.Line },
                                            { "pie", (int)OutputType.Pie},
                                            { "bar", (int)OutputType.Bar },
                                            { "column", (int)OutputType.Bar },
                                            {"combo",(int)OutputType.Combo }
                                        };

            if (reportTypeMap.TryGetValue(reportType, out int outputTypeId))
            {
                return outputTypeId;
            }

            throw new ArgumentException($"Invalid report type: {reportType}", nameof(reportType));
        }

        /// <summary>
        /// Asynchronously retrieves chart data for the given report details by making API calls and processing responses.
        /// </summary>
        /// <param name="reportDetail">The details of the report, including type and unique key.</param> 
        /// <param name="outputTypeId">The Id of the outputType of Report.</param>                
        /// <param name="filters">The filters that applies of Report.</param>
        /// <param name="selectedParameters">The selected Parametersthat applies of Report.</param>
        /// <param name="isUiExport">Flag for export</param>
        /// <param name="isScheduledReport">Indicates whether the report is a scheduled report.</param>
        /// <param name="callDto">The details of the request</param>
        /// <param name="cancellationToken"></param>
        /// <param name="sorting">The sorting that applies of Report.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the chart data as a string.</returns>        
        private async Task<string> GetChartDataAsync(DownloadReportResultDto reportDetail, int outputTypeId, string filters,
            List<SelectedParameterDto> selectedParameters, bool isUiExport, bool isScheduledReport, RunReportApiCallDto callDto,
            string sorting, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                string chartData = null;
                List<SelectedFilterDto> filtersList = null;

                if (filters != null)
                {
                    filtersList = JsonSerializer.Deserialize<List<SelectedFilterDto>>(filters);
                }

                List<SelectedSortDto> sortingList = null;
                if (sorting != null)
                {
                    sortingList = JsonSerializer.Deserialize<List<SelectedSortDto>>(sorting);
                }
                if (ReportConstant.ChartTypes.Contains(reportDetail.ReportType))
                {
                    outputTypeId = GetOutputTypeId(reportDetail.ReportType);
                    RunReportApiCallDto runchartDataParam = new RunReportApiCallDto
                    {
                        BaseReportXrmEntityId = reportDetail.EntityId,
                        UKey = reportDetail.ReportUkey,
                        OutputTypeId = outputTypeId,
                        PaginationDto = new PaginationandSearchingDto { PageSize = int.MaxValue, StartIndex = 1 },
                        ReportType = reportDetail.ReportType,
                        SelectedFilters = filtersList,
                        SelectedParameters = selectedParameters,
                        IsUiExport = isUiExport,
                        IsScheduledReport = isScheduledReport,
                        DrillDownRow = new List<ReportDataRowItemModelDto>(),
                        SelectedSorts = sortingList
                    };
                    if (!isScheduledReport && string.IsNullOrEmpty(callDto.UKey))
                    {
                        runchartDataParam.Json = callDto.Json;
                    }
                    var runChartData = await RunChartAsync(runchartDataParam);
                    cancellationToken.ThrowIfCancellationRequested();
                    if (runChartData is XrmSuccessWithDataResponse successResponse &&
                        successResponse.Data is ChartDataDto chartDataResultModel)
                    {
                        chartDataResultModel.OutPutTypeId = GetOutputTypeId(reportDetail.ReportType);
                        //var envChartURL = Environment.GetEnvironmentVariable(ReportConstant.ChartUrl);
                        var envChartURL = "https://nvxrm-testing.acrocorp.com/qa_ui/charts/9C8DEDFF-5F32-47DF-82E6-776005DCBAEB";
                        if (envChartURL == null)
                        {
                            _ = new XrmInternalServerErrorResponse("Unable to Retrive EnvChart Variable.", new Exception("Unable to Retrive EnvChart Variable."));
                            return null;
                        }
                        var chartDataResponse = await XrmDotNetReportHelper.CaptureElementScreenshotAsync(envChartURL, reportDetail.ReportName, runChartData);

                        if (chartDataResponse is XrmSuccessWithDataResponse chartSuccessResponse &&
                            chartSuccessResponse.Data is string chart)
                        {
                            chartData = chart;
                        }
                        else
                        {
                            _ = new XrmInternalServerErrorResponse(chartDataResponse.Message, new Exception(chartDataResponse.Message));
                            return null;
                        }
                    }
                    else
                    {
                        _ = new XrmInternalServerErrorResponse(runChartData.Message, new Exception(runChartData.Message));
                        return null;
                    }
                }

                return chartData;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
        }
    }
}
