/****** Object:  StoredProcedure [LOGGING].[SP_PIPELINE_RUN_LOG_START]    Script Date: 16-02-2025 22:27:33 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [LOGGING].[SP_PIPELINE_RUN_LOG_START]
( 
@vPipelineRunId nvarchar(100),
@vEntity nvarchar(100),
@vPipelineName nvarchar(100),
@vStartTime datetime
)
AS
BEGIN
	insert into LOGGING.PIPELINE_RUN_LOG
	values(@vPipelineRunId,@vEntity,@vPipelineName,'RUNNING',NULL,@vStartTime,NULL)
END
GO

