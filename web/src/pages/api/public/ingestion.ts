import {
  type AuthHeaderVerificationResult,
  verifyAuthHeaderAndReturnScope,
} from "@/src/features/public-api/server/apiAuth";
import { cors, runMiddleware } from "@/src/features/public-api/server/cors";
import { prisma, Prisma } from "@langfuse/shared/src/db";
import { type NextApiRequest, type NextApiResponse } from "next";
import { z } from "zod";
import {
  type ingestionApiSchema,
  eventTypes,
  ingestionEvent,
  traceEvent,
} from "@/src/features/public-api/server/ingestion-api-schema";
import { type ApiAccessScope } from "@/src/features/public-api/server/types";
import {
  persistEventBatchMiddleware,
  persistEventMiddleware,
} from "@/src/server/api/services/event-service";
import { backOff } from "exponential-backoff";
import { ResourceNotFoundError } from "@/src/utils/exceptions";
import {
  SdkLogProcessor,
  type EventProcessor,
  TraceProcessor,
  ObservationEvent,
} from "../../../server/api/services/EventProcessor";
import { ObservationProcessor } from "../../../server/api/services/EventProcessor";
import { ScoreProcessor } from "../../../server/api/services/EventProcessor";
import { isNotNullOrUndefined } from "@/src/utils/types";
import { telemetry } from "@/src/features/telemetry";
import { jsonSchema } from "@/src/utils/zod";
import * as Sentry from "@sentry/nextjs";
import { isPrismaException } from "@/src/utils/exceptions";
import { env } from "@/src/env.mjs";
import {
  ValidationError,
  MethodNotAllowedError,
  BaseError,
  ForbiddenError,
  UnauthorizedError,
  Model,
  Observation,
  arrayOptionsFilter,
} from "@langfuse/shared";
import { AlertDescription } from "@/src/components/ui/alert";
import { v4 } from "uuid";
import { mergeJson } from "@/src/utils/json";
import { update, merge } from "lodash";
import { da } from "date-fns/locale";

export const config = {
  api: {
    bodyParser: {
      sizeLimit: "4.5mb",
    },
  },
};

type BatchResult = {
  result: unknown;
  id: string;
  type: string;
};

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  try {
    await runMiddleware(req, res, cors);

    if (req.method !== "POST") throw new MethodNotAllowedError();

    // CHECK AUTH FOR ALL EVENTS
    const authCheck = await verifyAuthHeaderAndReturnScope(
      req.headers.authorization,
    );

    if (!authCheck.validKey) throw new UnauthorizedError(authCheck.error);

    const batchType = z.object({
      batch: z.array(z.unknown()),
      metadata: jsonSchema.nullish(),
    });

    const parsedSchema = batchType.safeParse(req.body);

    if (!parsedSchema.success) {
      console.log("Invalid request data", parsedSchema.error);
      return res.status(400).json({
        message: "Invalid request data",
        errors: parsedSchema.error.issues.map((issue) => issue.message),
      });
    }

    const validationErrors: { id: string; error: unknown }[] = [];

    const batch: (z.infer<typeof ingestionEvent> | undefined)[] =
      parsedSchema.data.batch.map((event) => {
        const parsed = ingestionEvent.safeParse(event);
        if (!parsed.success) {
          validationErrors.push({
            id:
              typeof event === "object" && event && "id" in event
                ? typeof event.id === "string"
                  ? event.id
                  : "unknown"
                : "unknown",
            error: new ValidationError(parsed.error.message),
          });
          return undefined;
        } else {
          return parsed.data;
        }
      });
    const filteredBatch: z.infer<typeof ingestionEvent>[] =
      batch.filter(isNotNullOrUndefined);

    await telemetry();

    const sortedBatch = sortBatch(filteredBatch);
    const result = await handleBatch(
      sortedBatch,
      parsedSchema.data.metadata,
      req,
      authCheck,
    );

    // send out REST requests to worker for all trace types
    await sendToWorkerIfEnvironmentConfigured(
      result.results,
      authCheck.scope.projectId,
    );

    handleBatchResult(
      [...validationErrors, ...result.errors],
      result.results,
      res,
    );
  } catch (error: unknown) {
    console.error("error handling ingestion event", error);

    if (error instanceof BaseError) {
      return res.status(error.httpCode).json({
        error: error.name,
        message: error.message,
      });
    }

    if (isPrismaException(error)) {
      return res.status(500).json({
        error: "Internal Server Error",
      });
    }
    if (error instanceof z.ZodError) {
      console.log(`Zod exception`, error.errors);
      return res.status(400).json({
        message: "Invalid request data",
        error: error.errors,
      });
    }

    const errorMessage =
      error instanceof Error ? error.message : "An unknown error occurred";
    res.status(500).json({
      message: "Invalid request data",
      errors: [errorMessage],
    });
  }
}

const sortBatch = (batch: Array<z.infer<typeof ingestionEvent>>) => {
  // keep the order of events as they are. Order events in a way that types containing updates come last
  // Filter out OBSERVATION_UPDATE events
  const updates = batch.filter(
    (event) => event.type === eventTypes.OBSERVATION_UPDATE,
  );

  // Keep all other events in their original order
  const others = batch.filter(
    (event) => event.type !== eventTypes.OBSERVATION_UPDATE,
  );

  // Return the array with non-update events first, followed by update events
  return [...others, ...updates];
};

const ENABLE_SQL_BATCH = true;

const findModelInMemory = (
  projectId: string,
  event: ObservationEvent,
  allModels: Model[],
  existingObservation: Observation | undefined,
): Model | undefined => {
  if (
    event.type != eventTypes.GENERATION_CREATE &&
    event.type != eventTypes.GENERATION_UPDATE
  ) {
    return undefined;
  }

  const model = event.body?.model;
  const unit = event.body?.usage?.unit;
  const startTime = event.body.startTime
    ? new Date(event.body.startTime)
    : undefined;

  const matches = allModels.filter((m) => {
    if (m.projectId != null && m.projectId != projectId) {
      return false;
    }

    if (model) {
      const matchPattern = m.matchPattern.replace("(?i)", "");
      const pattern = new RegExp(matchPattern, "i");

      if (!pattern.test(model)) {
        return false;
      }
    } else if (
      existingObservation &&
      existingObservation.internalModel != m.modelName
    ) {
      return false;
    }

    const mergedUnit = unit ?? existingObservation?.unit;

    if (mergedUnit && m.unit != mergedUnit) {
      return false;
    }

    if (startTime && m.startDate && m.startDate > startTime) {
      return false;
    }
  });

  // sort matches by startDate which are of type Date in descending order
  matches.sort((a, b) => {
    if (a.startDate && b.startDate) {
      return b.startDate.getTime() - a.startDate.getTime();
    }
    return 0;
  });

  return matches[0] || undefined;
};

export const handleSqlBatch = async (
  events: z.infer<typeof ingestionApiSchema>["batch"],
  metadata: z.infer<typeof ingestionApiSchema>["metadata"],
  req: NextApiRequest,
  apiScope: ApiAccessScope,
) => {
  if (apiScope.accessLevel != "all") {
    throw new ForbiddenError("Access denied. Creation of events not allowed.");
  }

  const cleanedEvents = events.map((event) =>
    ingestionEvent.parse(cleanEvent(event)),
  );

  // insert events into the DB
  persistEventBatchMiddleware(
    prisma,
    apiScope.projectId,
    req,
    cleanedEvents,
    metadata,
  );

  let results = [];

  const traceEvents = cleanedEvents.filter(
    (event): event is z.infer<typeof traceEvent> =>
      event.type === eventTypes.TRACE_CREATE,
  );

  for (const event of traceEvents) {
    const processor = new TraceProcessor(event);
    const result = await processor.process(apiScope);
    results.push({
      result,
      id: event.id,
      type: event.type,
    });
  }

  const observationEvents = cleanedEvents.filter(
    (event): event is ObservationEvent =>
      event.type === eventTypes.OBSERVATION_CREATE ||
      event.type === eventTypes.OBSERVATION_UPDATE ||
      event.type === eventTypes.EVENT_CREATE ||
      event.type === eventTypes.SPAN_CREATE ||
      event.type === eventTypes.SPAN_UPDATE ||
      event.type === eventTypes.GENERATION_CREATE ||
      event.type === eventTypes.GENERATION_UPDATE,
  );

  // find all observations by IDs to see which ones exist

  const observationIds = observationEvents
    .map((event) => event.body.id)
    .filter((v): v is string => v !== undefined && v != null);

  const observationTraceIds = observationEvents
    .map((event) => event.body.traceId)
    .filter((v): v is string => v !== undefined && v != null);

  // all of these will get update events
  const existingObservations = await prisma.observation.findMany({
    where: {
      id: {
        in: observationIds,
      },
    },
  });

  const projectMatch = existingObservations.every(
    (observation) => observation.projectId === apiScope.projectId,
  );

  if (!projectMatch) {
    throw new ForbiddenError("Access denied. Observation from another project");
  }

  // find all the relevant models
  const allModels = await prisma.model.findMany({});

  // TODO: this needs a type
  const newObservationsById = new Map<string, Observation>();
  const updateObservationsById = new Map<string, Observation>();

  for (const event of observationEvents) {
    let type: "EVENT" | "SPAN" | "GENERATION";
    switch (event.type) {
      case eventTypes.OBSERVATION_CREATE:
      case eventTypes.OBSERVATION_UPDATE:
        throw new Error("observation events not supported in batch sql");
      case eventTypes.EVENT_CREATE:
        type = "EVENT" as const;
        break;
      case eventTypes.SPAN_CREATE:
      case eventTypes.SPAN_UPDATE:
        type = "SPAN" as const;
        break;
      case eventTypes.GENERATION_CREATE:
      case eventTypes.GENERATION_UPDATE:
        type = "GENERATION" as const;
        break;
    }

    let existingObservation = newObservationsById.get(event.body.id);

    let dbObservation = null;

    if (!existingObservation) {
      existingObservation = existingObservations.find(
        (observation) => observation.id === event.body.id,
      );
      dbObservation = existingObservation;
    }

    const internalModel = findModelInMemory(
      apiScope.projectId,
      event,
      allModels,
      existingObservation,
    );

    if (!event.body.traceId && !existingObservation) {
      throw new ValidationError(
        `Observation with ID ${event.body.id} does not have a trace`,
      );
    }

    const processor = new ObservationProcessor(event);

    const [newInputCount, newOutputCount] =
      "usage" in event.body
        ? processor.calculateTokenCounts(
            event.body,
            internalModel ?? undefined,
            existingObservation ?? undefined,
          )
        : [undefined, undefined];

    // now we should either put an observation into the newObservations list.
    // This doesn't pass type check later for some reason
    const mergedMetadata = mergeJson(
      existingObservation?.metadata
        ? jsonSchema.parse(existingObservation.metadata)
        : undefined,
      event.body.metadata ?? undefined,
    );

    const observation: Partial<Observation> = {
      id: event.body.id ?? v4(),
      traceId: event.body.traceId || existingObservation?.traceId,
      type: type,
      name: event.body.name,
      startTime: event.body.startTime
        ? new Date(event.body.startTime)
        : undefined,
      endTime:
        "endTime" in event.body && event.body.endTime
          ? new Date(event.body.endTime)
          : undefined,
      completionStartTime:
        "completionStartTime" in event.body && event.body.completionStartTime
          ? new Date(event.body.completionStartTime)
          : undefined,
      metadata: mergedMetadata ?? undefined,
      model: "model" in event.body ? event.body.model : undefined,
      modelParameters:
        "modelParameters" in event.body
          ? event.body.modelParameters
          : undefined,
      input: event.body.input ?? undefined,
      output: event.body.output ?? undefined,
      promptTokens: newInputCount,
      completionTokens: newOutputCount,
      totalTokens:
        "usage" in event.body
          ? event.body.usage?.total ??
            (newInputCount ?? 0) + (newOutputCount ?? 0)
          : undefined,
      unit:
        "usage" in event.body
          ? event.body.usage?.unit ?? internalModel?.unit
          : internalModel?.unit,
      level: event.body.level ?? undefined,
      statusMessage: event.body.statusMessage ?? undefined,
      parentObservationId: event.body.parentObservationId ?? undefined,
      version: event.body.version ?? undefined,
      projectId: apiScope.projectId,
      promptId: undefined, // TODO: write logic for this.
      internalModel: internalModel ? internalModel.modelName : undefined,
      inputCost:
        "usage" in event.body && event.body.usage?.inputCost
          ? new Prisma.Decimal(event.body.usage?.inputCost)
          : undefined,
      outputCost:
        "usage" in event.body && event.body.usage?.outputCost
          ? new Prisma.Decimal(event.body.usage?.outputCost)
          : undefined,
      totalCost:
        "usage" in event.body && event.body.usage?.totalCost
          ? new Prisma.Decimal(event.body.usage?.totalCost)
          : undefined,
    };

    // it's in the dictionary
    if (newObservationsById.has(event.body.id)) {
      const observationToUpdate = merge(
        {},
        newObservationsById.get(event.body.id),
        observation,
      );
      newObservationsById.set(event.body.id, observationToUpdate);
    } else if (updateObservationsById.has(event.body.id)) {
      // have to be careful updating undefined keys here.
      const observationToUpdate = merge(
        {},
        updateObservationsById.get(event.body.id),
        observation,
      );

      updateObservationsById.set(event.body.id, observationToUpdate);
    } else if (dbObservation) {
      // it's in neither dictionary, but it's in the DB

      const observationToUpdate = merge({}, dbObservation, observation);

      updateObservationsById.set(event.body.id, observationToUpdate);
    } else {
      // it's not in any of the dictionaries, and it's not in the DB
      newObservationsById.set(event.body.id, observation as Observation);
    }
  }

  let newObservations = null;

  if (newObservationsById.size > 0) {
    newObservations = await prisma.observation.createMany({
      // @ts-ignore
      data: Array.from(newObservationsById.values()),
    });
  }

  const updates = Array.from(updateObservationsById.entries()).map(
    ([id, observation]) =>
      prisma.observation.update({
        where: { id },
        // @ts-ignore
        data: observation,
      }),
  );

  if (updates.length > 0) {
    await prisma.$transaction(updates);
  }

  // I have newObservationsById and updateObservationsById. I want to make a bulk prisma call
  // to create or update these models in the DB

  return {
    results: Array.from(newObservationsById.values())
      .concat(Array.from(updateObservationsById.values()))
      .map((o) => ({
        result: o,
        id: o.id,
        type: o.type,
      })),
    errors: [],
  };
};

export const handleBatch = async (
  events: z.infer<typeof ingestionApiSchema>["batch"],
  metadata: z.infer<typeof ingestionApiSchema>["metadata"],
  req: NextApiRequest,
  authCheck: AuthHeaderVerificationResult,
) => {
  console.log(`handling ingestion event with ${events.length} events`);

  if (!authCheck.validKey) throw new UnauthorizedError(authCheck.error);

  let errors: {
    error: unknown;
    id: string;
    type: string;
  }[] = []; // Array to store the errors

  let results: BatchResult[] = []; // Array to store the results

  if (ENABLE_SQL_BATCH) {
    const batch = await handleSqlBatch(events, metadata, req, authCheck.scope);

    results = batch.results;
    errors = batch.errors;
  } else {
    for (const singleEvent of events) {
      try {
        const result = await retry(async () => {
          return await handleSingleEvent(
            singleEvent,
            metadata,
            req,
            authCheck.scope,
          );
        });
        results.push({
          result: result,
          id: singleEvent.id,
          type: singleEvent.type,
        }); // Push each result into the array
      } catch (error) {
        // Handle or log the error if `handleSingleEvent` fails
        console.error("Error handling event:", error);
        // Decide how to handle the error: rethrow, continue, or push an error object to results
        // For example, push an error object:
        errors.push({
          error: error,
          id: singleEvent.id,
          type: singleEvent.type,
        });
      }
    }
  }

  return { results, errors };
};

async function retry<T>(request: () => Promise<T>): Promise<T> {
  return await backOff(request, {
    numOfAttempts: 3,
    retry: (e: Error, attemptNumber: number) => {
      if (e instanceof UnauthorizedError || e instanceof ForbiddenError) {
        console.log("not retrying auth error");
        return false;
      }
      console.log(`retrying processing events ${attemptNumber}`);
      return true;
    },
  });
}
export const getBadRequestError = (errors: Array<unknown>): ValidationError[] =>
  errors.filter(
    (error): error is ValidationError => error instanceof ValidationError,
  );

export const getResourceNotFoundError = (
  errors: Array<unknown>,
): ResourceNotFoundError[] =>
  errors.filter(
    (error): error is ResourceNotFoundError =>
      error instanceof ResourceNotFoundError,
  );

export const hasBadRequestError = (errors: Array<unknown>) =>
  errors.some((error) => error instanceof ValidationError);

const handleSingleEvent = async (
  event: z.infer<typeof ingestionEvent>,
  metadata: z.infer<typeof ingestionApiSchema>["metadata"],
  req: NextApiRequest,
  apiScope: ApiAccessScope,
) => {
  console.log(`handling single event ${event.id} ${event.type}`);

  const cleanedEvent = ingestionEvent.parse(cleanEvent(event));

  const { type } = cleanedEvent;

  await persistEventMiddleware(
    prisma,
    apiScope.projectId,
    req,
    cleanedEvent,
    metadata,
  );

  let processor: EventProcessor;
  switch (type) {
    case eventTypes.TRACE_CREATE:
      processor = new TraceProcessor(cleanedEvent);
      break;
    case eventTypes.OBSERVATION_CREATE:
    case eventTypes.OBSERVATION_UPDATE:
    case eventTypes.EVENT_CREATE:
    case eventTypes.SPAN_CREATE:
    case eventTypes.SPAN_UPDATE:
    case eventTypes.GENERATION_CREATE:
    case eventTypes.GENERATION_UPDATE:
      processor = new ObservationProcessor(cleanedEvent);
      break;
    case eventTypes.SCORE_CREATE: {
      processor = new ScoreProcessor(cleanedEvent);
      break;
    }
    case eventTypes.SDK_LOG:
      processor = new SdkLogProcessor(cleanedEvent);
  }

  // Deny access to non-score events if the access level is not "all"
  // This is an additional safeguard to auth checks in EventProcessor
  if (apiScope.accessLevel !== "all" && type !== eventTypes.SCORE_CREATE) {
    throw new ForbiddenError("Access denied. Event type not allowed.");
  }

  return await processor.process(apiScope);
};

export const handleBatchResult = (
  errors: Array<{ id: string; error: unknown }>,
  results: Array<{ id: string; result: unknown }>,
  res: NextApiResponse,
) => {
  const returnedErrors: {
    id: string;
    status: number;
    message?: string;
    error?: string;
  }[] = [];

  const successes: {
    id: string;
    status: number;
  }[] = [];

  errors.forEach((error) => {
    if (error.error instanceof ValidationError) {
      returnedErrors.push({
        id: error.id,
        status: 400,
        message: "Invalid request data",
        error: error.error.message,
      });
    } else if (error.error instanceof UnauthorizedError) {
      returnedErrors.push({
        id: error.id,
        status: 401,
        message: "Authentication error",
        error: error.error.message,
      });
    } else if (error.error instanceof ResourceNotFoundError) {
      returnedErrors.push({
        id: error.id,
        status: 404,
        message: "Resource not found",
        error: error.error.message,
      });
    } else {
      if (process.env.NEXT_PUBLIC_SENTRY_DSN) {
        Sentry.captureException(error.error);
      }
      returnedErrors.push({
        id: error.id,
        status: 500,
        error: "Internal Server Error",
      });
    }
  });

  if (returnedErrors.length > 0) {
    console.log("Error processing events", returnedErrors);
  }

  results.forEach((result) => {
    successes.push({
      id: result.id,
      status: 201,
    });
  });

  return res.status(207).send({ errors: returnedErrors, successes });
};

export const handleBatchResultLegacy = (
  errors: Array<{ id: string; error: unknown }>,
  results: Array<{ id: string; result: unknown }>,
  res: NextApiResponse,
) => {
  const unknownErrors = errors.map((error) => error.error);

  const badRequestErrors = getBadRequestError(unknownErrors);
  if (badRequestErrors.length > 0) {
    console.log("Bad request errors", badRequestErrors);
    return res.status(400).json({
      message: "Invalid request data",
      errors: badRequestErrors.map((error) => error.message),
    });
  }

  const ResourceNotFoundError = getResourceNotFoundError(unknownErrors);
  if (ResourceNotFoundError.length > 0) {
    return res.status(404).json({
      message: "Resource not found",
      errors: ResourceNotFoundError.map((error) => error.message),
    });
  }

  if (errors.length > 0) {
    console.log("Error processing events", unknownErrors);
    return res.status(500).json({
      errors: ["Internal Server Error"],
    });
  }
  return res.status(200).send(results.length > 0 ? results[0]?.result : {});
};

// cleans NULL characters from the event
export function cleanEvent(obj: unknown): unknown {
  if (typeof obj === "string") {
    return obj.replace(/\u0000/g, "");
  } else if (typeof obj === "object" && obj !== null) {
    if (Array.isArray(obj)) {
      return obj.map(cleanEvent);
    } else {
      // Here we assert that obj is a Record<string, unknown>
      const objAsRecord = obj as Record<string, unknown>;
      const newObj: Record<string, unknown> = {};
      for (const key in objAsRecord) {
        newObj[key] = cleanEvent(objAsRecord[key]);
      }
      return newObj;
    }
  } else {
    return obj;
  }
}

export const sendToWorkerIfEnvironmentConfigured = async (
  batchResults: BatchResult[],
  projectId: string,
): Promise<void> => {
  try {
    if (
      env.LANGFUSE_WORKER_HOST &&
      env.LANGFUSE_WORKER_PASSWORD &&
      env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION
    ) {
      const traceEvents = batchResults
        .filter((result) => result.type === eventTypes.TRACE_CREATE) // we only have create, no update.
        .map((result) =>
          result.result &&
          typeof result.result === "object" &&
          "id" in result.result
            ? // ingestion API only gets traces for one projectId
              { traceId: result.result.id, projectId: projectId }
            : null,
        )
        .filter(isNotNullOrUndefined);

      if (traceEvents.length > 0) {
        await fetch(`${env.LANGFUSE_WORKER_HOST}/api/events`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization:
              "Basic " +
              Buffer.from(
                "admin" + ":" + env.LANGFUSE_WORKER_PASSWORD,
              ).toString("base64"),
          },
          body: JSON.stringify(traceEvents),
        });
      }
    }
  } catch (error) {
    console.error("Error sending events to worker", error);
  }
};
