/****** Object:  StoredProcedure [LOGGING].[SP_PIPELINE_RUN_LOG_END]    Script Date: 16-02-2025 22:28:44 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [LOGGING].[SP_PIPELINE_RUN_LOG_END]
( 
@vPipelineRunId nvarchar(100),
@vPipelineStatus nvarchar(10),
@vEntity nvarchar(100),
@vRecordsProcessed int,
@vEndTime datetime
)
AS
BEGIN
	UPDATE LOGGING.PIPELINE_RUN_LOG
		SET Pipeline_Status=@vPipelineStatus,
		Records_Processed=@vRecordsProcessed,
		End_Time=@vEndTime
	WHERE Pipeline_Run_Id=@vPipelineRunId
	and entity=@vEntity
END
GO

