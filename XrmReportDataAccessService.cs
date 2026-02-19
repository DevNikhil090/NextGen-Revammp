using APIService.ReportService.XrmDotNetReport.Interfaces;
using APIService.ReportService.XrmDotNetReport.Models;
using APIService.ReportService.XrmDotNetReport.Repository;
using CommonUtilities.Enums;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace APIService.ReportService.XrmDotNetReport.Service
{
    /// <summary>
    /// Service to apply data access rights and handle XRM report-related functionality.
    /// </summary>
    public class XrmReportDataAccessService
    {
        /// <summary>
        /// Applies data access rights for a stored procedure based on the user's permissions.
        /// </summary>
        /// <param name="sqlQuery">The SQL query for the stored procedure.</param>
        /// <param name="serviceProvider">Service provider to retrieve the necessary services for data access.</param>
        /// <param name="userNo">The user number for which data access rights are to be applied.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>A collection of valid record IDs for the specified stored procedure.</returns>
        public static async Task<Dictionary<string, IEnumerable<int>>> ApplyDataAccessRightsForSP(string sqlQuery, IServiceProvider serviceProvider, int userNo, CancellationToken cancellationToken = default)
        {
            try
            {
                
                var sqlTables = XrmDotNetReportHelper.SplitSqlSps(sqlQuery, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                var result = new Dictionary<string, IEnumerable<int>>();
                IEnumerable<int> validRecordIds = new List<int>();

                foreach (KeyValuePair<XrmEntities, List<string>> entity in GetEntityTableName(sqlTables, false, cancellationToken))
                {
                    var entityPrimaryKey = GetEntityPrimaryColumnName(entity.Key);

                    cancellationToken.ThrowIfCancellationRequested();
                    if (string.IsNullOrEmpty(entityPrimaryKey)) { continue; }

                    validRecordIds = await GetEntityValidRecordIds(entity.Key, serviceProvider, userNo, cancellationToken);

                    cancellationToken.ThrowIfCancellationRequested();
                    var entityCount = result.Count(x => x.Key == entityPrimaryKey);

                    result.Add(entityPrimaryKey + (entityCount > 0 ? entityCount.ToString() : string.Empty), validRecordIds.Distinct());
                }
                return result;
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (Exception)
            {
                return null;
            }
        }


        /// <summary>
        /// Modifies the provided SQL query to include data access rights based on the user's permissions.
        /// </summary>
        /// <param name="sqlQuery">The original SQL query to which data access rights will be applied.</param>
        /// <param name="serviceProvider">Service provider to retrieve the necessary services for data access.</param>
        /// <param name="userNo">The userNo for which DAR to be applied, default will be considered for the logged-in user</param>
        /// <param name="isLookupCall">Set flag for lookup table</param>
        /// <param name="baseEntityId">Set flag for base entity Id to apply the DAR</param>
        /// <param name="cancellationToken"></param>
        /// <returns>A modified SQL query with data access conditions applied.</returns>
        public static async Task<(string, Dictionary<string, IEnumerable<int>>, string)> ApplyDataAccessRightsOptimized(string sqlQuery, IServiceProvider serviceProvider, int userNo, bool isLookupCall = false, int? baseEntityId = null, CancellationToken cancellationToken = default)
        {
            try
            {
                var sqlTables = XrmDotNetReportHelper.SplitSqlTables(sqlQuery);
                var darTables = new Dictionary<string, IEnumerable<int>>();
                foreach (var entity in GetEntityTableName(sqlTables, isLookupCall, cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (isLookupCall || (baseEntityId > 0 && (entity.Key == (XrmEntities)baseEntityId) || (entity.Key == XrmEntities.StaffingAgency && XrmEntities.MarkupConfiguration == (XrmEntities)baseEntityId)))
                    {
                        var entityPrimaryKey = GetEntityPrimaryColumnName(entity.Key, cancellationToken);

                        // Either DAR is not implemented or not needed for the Entity if primary key is not available.
                        if (string.IsNullOrEmpty(entityPrimaryKey)) { continue; }

                        var entityValidRecordIds = await GetEntityValidRecordIds(entity.Key, serviceProvider, userNo, cancellationToken);

                        darTables.Add(entityPrimaryKey, entityValidRecordIds.Distinct());

                        foreach (var table in entity.Value)
                        {
                            string condition = $"[{table}].[{entityPrimaryKey}] = {entityPrimaryKey}.Id ";

                            // List of possible SQL clauses to check for
                            string[] sqlClauses = { "GROUP BY", "ORDER BY", "HAVING" };

                            // Find the position of the first occurrence of any SQL clause after WHERE
                            int insertPosition = sqlQuery.Length; // Default to end of the string
                            foreach (string clause in sqlClauses)
                            {
                                int clausePosition = Regex.Match(sqlQuery, $@"\b{clause}\b", RegexOptions.IgnoreCase, TimeSpan.FromMilliseconds(100)).Index;
                                if (clausePosition > 0 && clausePosition < insertPosition)
                                {
                                    insertPosition = clausePosition; // Find the earliest position of a clause
                                }
                            }

                            // Check if the query already contains a WHERE clause
                            if (Regex.IsMatch(sqlQuery, @"\bWHERE\b", RegexOptions.IgnoreCase, TimeSpan.FromMilliseconds(100)))
                            {
                                sqlQuery = sqlQuery.Insert(insertPosition, " AND " + condition);
                            }
                            else
                            {
                                sqlQuery = sqlQuery.Insert(insertPosition, " WHERE " + condition);
                            }
                        }
                        // Ensure self-join is added before WHERE clause
                        int lastWhereIndex = sqlQuery.LastIndexOf("WHERE", StringComparison.OrdinalIgnoreCase);

                        if (lastWhereIndex >= 0)
                        {
                            // Insert the alias just before the last WHERE clause
                            sqlQuery = sqlQuery.Insert(lastWhereIndex, $", #{entityPrimaryKey} AS {entityPrimaryKey} ");
                        }


                    }
                }
                cancellationToken.ThrowIfCancellationRequested();

                return (sqlQuery, darTables, string.Empty);
            }
            catch (OperationCanceledException)
            {
                return (string.Empty, new Dictionary<string, IEnumerable<int>>(), string.Empty);
            }
        }

        private static DataTable CreateDataTable(IEnumerable<int> values)
        {
            DataTable table = new DataTable();
            table.Columns.Add("Value", typeof(int));

            foreach (var value in values.Distinct())
            {
                table.Rows.Add(value);
            }

            return table;
        }

        /// <summary>
        /// Retrieves valid record IDs for a specific entity based on the user's data access rights.
        /// </summary>
        /// <param name="xrmEntity">The XRM entity for which valid record IDs are retrieved.</param>
        /// <param name="serviceProvider">Service provider to retrieve the required repository for data access.</param>
        /// <param name="userNo">The userNo for which DAR to be applied, default will be considered for the logged-in user</param>
        /// <param name="cancellationToken"></param>
        /// <returns>A collection of valid record IDs for the specified entity.</returns>
        public static async ValueTask<IEnumerable<int>> GetEntityValidRecordIds(XrmEntities xrmEntity, IServiceProvider serviceProvider, int userNo, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var obj = serviceProvider.GetService<IXrmReportDataAccessRepository>();
                obj.UserNo = userNo;

                switch (xrmEntity)
                {
                    case XrmEntities.Assignment:
                        return await obj.AssignmentIdsAsync(cancellationToken);
                    case XrmEntities.JobCategory:
                        return await obj.JobCategoryIdsAsync(cancellationToken);
                    case XrmEntities.LaborCategory:
                        return await obj.LaborCategoryIdsAsync(cancellationToken);
                    case XrmEntities.LIRequest:
                        return await obj.LIRequestIdsAsync(cancellationToken);
                    case XrmEntities.Location:
                        return await obj.LocationIdsAsync(cancellationToken);
                    case XrmEntities.RequisitionLibrary:
                        return await obj.ReqLibraryIdsAsync(cancellationToken);
                    case XrmEntities.Sector:
                        return await obj.SectorIdsAsync(cancellationToken);
                    case XrmEntities.ContractorEvent:
                        return await obj.WorkerEventIdsAsync(cancellationToken);
                    case XrmEntities.ReasonForRequest:
                        return await obj.ReasonForRequestIdsAsync(cancellationToken);
                    case XrmEntities.TimeEntry:
                        return await obj.TimeEntryIdsAsync(cancellationToken);
                    case XrmEntities.LiCandidate:
                        return await obj.CandidateIdsAsync(cancellationToken);
                    case XrmEntities.CandidateDeclineReason:
                        return await obj.CandidateDeclineReasonIdsAsync(cancellationToken);
                    case XrmEntities.Shift:
                        return await obj.ShiftIdsAsync(cancellationToken);
                    case XrmEntities.MinimumClearanceToStart:
                        return await obj.MinimumClearanceIdsAsync(cancellationToken);
                    case XrmEntities.RetireeOption:
                        return await obj.RetireeOptionIdsAsync(cancellationToken);
                    case XrmEntities.CandidateSelectionReason:
                        return await obj.CandidateSelectionReasonIdsAsync(cancellationToken);
                    case XrmEntities.RequestCancelCloseReason:
                        return await obj.RequestCancelCloseReasonIdsAsync(cancellationToken);
                    case XrmEntities.ExpenseEntry:
                        return await obj.ExpenseEntryIdsAsync(cancellationToken);
                    case XrmEntities.CostAccountingCode:
                        return await obj.CostAccountingCodeIdsAsync(cancellationToken);
                    case XrmEntities.StaffingAgency:
                        return await obj.StaffingAgencyIdsAsync(cancellationToken);
                    case XrmEntities.CLP:
                        return await obj.WorkerIdsAsync(cancellationToken);
                    case XrmEntities.ExpenseType:
                        return await obj.ExpenseTypeIdsAsync(cancellationToken);
                    case XrmEntities.TerminationReason:
                        return await obj.TerminationReasonIdsAsync(cancellationToken);
                    case XrmEntities.OrgLevel1:
                        return await obj.OrgLevel1IdsAsync(cancellationToken);
                    case XrmEntities.OrgLevel2:
                        return await obj.OrgLevel2IdsAsync(cancellationToken);
                    case XrmEntities.OrgLevel3:
                        return await obj.OrgLevel3IdsAsync(cancellationToken);
                    case XrmEntities.OrgLevel4:
                        return await obj.OrgLevel4IdsAsync(cancellationToken);
                    case XrmEntities.Revision:
                        return await obj.RevisionIdsAsync(cancellationToken);
                    case XrmEntities.Users:
                        return await obj.UserIdsAsync(cancellationToken);
                    case XrmEntities.EventReason:
                        return await obj.EventReasonIdsAsync(cancellationToken);
                    case XrmEntities.EventConfig:
                        return await obj.EventConfigIdsAsync(cancellationToken);
                    case XrmEntities.CandidatePool:
                        return await obj.CandidatePoolIdsAsync(cancellationToken);
                    case XrmEntities.MarkupConfiguration:
                        return await obj.MarkupConfigurationIdsAsync(cancellationToken);
                    case XrmEntities.Invoice:
                        return await obj.InvoiceIdsAsync(cancellationToken);
                    case XrmEntities.ProfessionalRequest:
                        return await obj.ProfessionalRequestIdsAsync(cancellationToken);
                    case XrmEntities.Submittals:
                        return await obj.SubmittalIdsAsync(cancellationToken);
                    case XrmEntities.InterviewRequest:
                        return await obj.InterviewRequestIdsAsync(cancellationToken);
                    case XrmEntities.SupplierPayment:
                        return await obj.SupplierPaymentIdsAsync(cancellationToken);
                    case XrmEntities.SOWRequest:
                        return await obj.SOWRequestIdsAsync(cancellationToken);
                    case XrmEntities.SOWResource:
                        return await obj.SOWRequestIdsAsync(cancellationToken);
                    case XrmEntities.SOWDeliverableReport:
                        return await obj.SOWRequestIdsAsync(cancellationToken);
                    case XrmEntities.CertificationAndTraining:
                        return await obj.CertificationAndTrainingIdsAsync(cancellationToken);
                    case XrmEntities.Survey:
                        return await obj.SurveyIdsAsync(cancellationToken);
                    case XrmEntities.PoConfiguration:
                        return await obj.PoConfigurationIdsAsync(cancellationToken);
                    case XrmEntities.DeliverableCompletion:
                        return await obj.DeliverableCompletionIdsAsync(cancellationToken);
                    case XrmEntities.EventLog:
                        return await obj.EventLogIdsAsync(cancellationToken);
                    case XrmEntities.EventLogEntity:
                        return await obj.EventLogEntityIdsAsync(cancellationToken);
                    case XrmEntities.EventLogEntityMainAction:
                        return await obj.EventLogEntityMainActionIdsAsync(cancellationToken);
                }
                return new List<int>();
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        /// <summary>
        /// Retrieves the primary column name for the specified XRM entity.
        /// </summary>
        /// <param name="xrmEntity">The XRM entity for which the primary column name is retrieved.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>The primary column name as a string.</returns>
        private static string GetEntityPrimaryColumnName(XrmEntities xrmEntity, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                switch (xrmEntity)
                {
                    case XrmEntities.Assignment:
                        return "AssignmentId";
                    case XrmEntities.JobCategory:
                        return "JobCategoryId";
                    case XrmEntities.LaborCategory:
                        return "LaborCategoryId";
                    case XrmEntities.LIRequest:
                        return "RequestId";
                    case XrmEntities.Location:
                        return "LocationId";
                    case XrmEntities.RequisitionLibrary:
                        return "ReqLibraryId";
                    case XrmEntities.Sector:
                        return "SectorId";
                    case XrmEntities.ContractorEvent:
                        return "WorkerEventId";
                    case XrmEntities.ReasonForRequest:
                        return "ReasonForRequestId";
                    case XrmEntities.TimeEntry:
                        return "TimeEntryId";
                    case XrmEntities.LiCandidate:
                        return "CandidateId";
                    case XrmEntities.CandidateDeclineReason:
                        return "CandidateDeclineReasonId";
                    case XrmEntities.Shift:
                        return "ShiftId";
                    case XrmEntities.MinimumClearanceToStart:
                        return "MinimumClearanceToStartId";
                    case XrmEntities.RetireeOption:
                        return "RetireeOptionId";
                    case XrmEntities.CandidateSelectionReason:
                        return "CandidateSelectionReasonId";
                    case XrmEntities.RequestCancelCloseReason:
                        return "RequestCancelCloseReasonId";
                    case XrmEntities.ExpenseEntry:
                        return "ExpenseId";
                    case XrmEntities.CostAccountingCode:
                        return "CostAccountingCodeId";
                    case XrmEntities.StaffingAgency:
                        return "StaffingAgencyId";
                    case XrmEntities.CLP:
                        return "WorkerId";
                    case XrmEntities.ExpenseType:
                        return "ExpenseTypeId";
                    case XrmEntities.TerminationReason:
                        return "TerminationReasonId";
                    case XrmEntities.OrgLevel1:
                        return "OrgLevel1Id";
                    case XrmEntities.OrgLevel2:
                        return "OrgLevel2Id";
                    case XrmEntities.OrgLevel3:
                        return "OrgLevel3Id";
                    case XrmEntities.OrgLevel4:
                        return "OrgLevel4Id";
                    case XrmEntities.Revision:
                        return "RevisionId";
                    case XrmEntities.Users:
                        return "UserId";
                    case XrmEntities.EventReason:
                        return "EventReasonId";
                    case XrmEntities.CandidatePool:
                        return "CandidatePoolId";
                    case XrmEntities.EventConfig:
                        return "EventConfigId";
                    case XrmEntities.MarkupConfiguration:
                        return "SectorId";
                    case XrmEntities.Invoice:
                        return "InvoiceId";
                    case XrmEntities.ProfessionalRequest:
                        return "RequestId";
                    case XrmEntities.Submittals:
                        return "SubmittalId";
                    case XrmEntities.InterviewRequest:
                        return "InterviewId";
                    case XrmEntities.SupplierPayment:
                        return "PaymentId";
                    case XrmEntities.SOWRequest:
                        return "SOWId";
                    case XrmEntities.SOWResource:
                        return "SOWId";
                    case XrmEntities.SOWDeliverableReport:
                        return "SOWId";
                    case XrmEntities.CertificationAndTraining:
                        return "TrainingId";
                    case XrmEntities.Survey:
                        return "SurveyId";
                    case XrmEntities.PoConfiguration:
                        return "PoId";
                    case XrmEntities.DeliverableCompletion:
                        return "DeleverableCompletionId";
                    case XrmEntities.EventLog:
                        return "EventLogId";
                    case XrmEntities.EventLogEntity:
                        return "EventLogEntityId";
                    case XrmEntities.EventLogEntityMainAction:
                        return "EventLogEntityMainActionId";
                }
                return string.Empty;
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        /// <summary>
        /// Retrieves the entity table names that are relevant to the provided SQL tables.
        /// </summary>
        /// <param name="sqlTables">List of SQL tables used in the query.</param>
        /// <param name="isLookupCall"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>A dictionary mapping XRM entities to their corresponding table names.</returns>
        private static Dictionary<XrmEntities, List<string>> GetEntityTableName(List<string> sqlTables, bool isLookupCall = false, CancellationToken cancellationToken = default)
        {
            try
            {

                cancellationToken.ThrowIfCancellationRequested();
                var dbDal = new DBHelper();

                var sql = "SELECT XrmEntityId, TableName FROM rpt.ReportEntityTables where Disabled = " + (isLookupCall ? "1" : "0");
                cancellationToken.ThrowIfCancellationRequested();
                var ds = new DataSet();
                if (cancellationToken.IsCancellationRequested != true)
                {
                    ds = dbDal.ExecuteDataSetAsync(sql, null, System.Data.CommandType.Text, cancellationToken).GetAwaiter().GetResult();
                    cancellationToken.ThrowIfCancellationRequested();
                }
                else
                {
                    ds = dbDal.ExecuteDataSet(sql, null, System.Data.CommandType.Text);
                    cancellationToken.ThrowIfCancellationRequested();
                }
                cancellationToken.ThrowIfCancellationRequested();
                var dict = new Dictionary<XrmEntities, List<string>>();

                if (ds != null && ds.Tables.Count > 0)
                {
                    var dataTable = ds.Tables[0];
                    if (dataTable.Columns.Contains("XrmEntityId") && dataTable.Columns.Contains("TableName"))
                    {
                        foreach (var table in sqlTables)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            var filterExpression = $"TableName = '{table.Replace("UdfValues", "UdfFields")}'";

                            var rows = dataTable.Select(filterExpression);

                            if (rows.Length > 0)
                            {
                                var entity = (XrmEntities)rows[0]["XrmEntityId"];
                                if (!dict.ContainsKey(entity))
                                {
                                    cancellationToken.ThrowIfCancellationRequested();
                                    var missingTimeSheetTables = new HashSet<string> { "GetMissingTimeSheets", "GetMissingTimeSheetsClient", "GetMissingTimeSheetsStaffing", "GetTimesheetUploadTemplate" };
                                    if (missingTimeSheetTables.Contains(table))
                                    {
                                        entity = XrmEntities.Assignment;
                                    }

                                    if (table == "GetCombinedPifData") //PIF report required to apply time and expense dar.
                                    {
                                        if (!dict.ContainsKey(XrmEntities.ExpenseEntry) || !dict.ContainsKey(XrmEntities.DeliverableCompletion))
                                        {
                                            dict.Add(XrmEntities.ExpenseEntry, new List<string>() { table });
                                            dict.Add(XrmEntities.DeliverableCompletion, new List<string>() { table });
                                        }
                                    }

                                    var werTables = new HashSet<string> { "GetWERReports", "GetWERReportsStaffing", "GetWERReportsClient" };
                                    if (werTables.Contains(table)) //WER report required to apply time and expense dar.
                                    {
                                        if (!dict.ContainsKey(XrmEntities.ExpenseEntry))
                                        {
                                            dict.Add(XrmEntities.ExpenseEntry, new List<string>() { table });
                                        }
                                    }
                                    cancellationToken.ThrowIfCancellationRequested();
                                    if (table == "GetRequestionBroadcast") //RequestionBroadcast report required to apply LI and Prof dar.
                                    {
                                        if (!dict.ContainsKey(XrmEntities.LIRequest))
                                        {
                                            dict.Add(XrmEntities.LIRequest, new List<string>() { table });
                                        }
                                    }

                                    if (!dict.ContainsKey(entity))
                                    {
                                        dict.Add(entity, new List<string>() { table });
                                    }
                                    else
                                    {
                                        dict[entity].Add(table);
                                    }

                                }
                                else
                                {
                                    dict[entity].Add(table);
                                }
                            }
                        }
                    }
                }
                else
                {
                    throw new Exception("No tables were found in the dataset.");
                }

                if (dict.Any(x => x.Key == XrmEntities.MarkupConfiguration))
                {
                    var lastData = dict.ToList(); // Convert dictionary to list to avoid modification during iteration
                    foreach (var item in lastData)
                    {
                        if (!dict.ContainsKey(XrmEntities.StaffingAgency))
                        {
                            dict.Add(XrmEntities.StaffingAgency, item.Value);
                        }
                    }
                }

                return dict;
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }
    }
}

