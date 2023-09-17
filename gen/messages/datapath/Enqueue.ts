// @ts-nocheck
import {
  tsValueToJsonValueFns,
  jsonValueToTsValueFns,
} from "../../runtime/json/scalar.ts";
import {
  WireMessage,
} from "../../runtime/wire/index.ts";
import {
  default as serialize,
} from "../../runtime/wire/serialize.ts";
import {
  tsValueToWireValueFns,
  wireValueToTsValueFns,
  unpackFns,
} from "../../runtime/wire/scalar.ts";
import {
  default as deserialize,
} from "../../runtime/wire/deserialize.ts";

export declare namespace $.datapath {
  export type Enqueue = {
    payload: Uint8Array;
    deadlineMs: string;
    kvKeysIfUndelivered: Uint8Array[];
    backoffSchedule: number[];
  }
}

export type Type = $.datapath.Enqueue;

export function getDefaultValue(): $.datapath.Enqueue {
  return {
    payload: new Uint8Array(),
    deadlineMs: "0",
    kvKeysIfUndelivered: [],
    backoffSchedule: [],
  };
}

export function createValue(partialValue: Partial<$.datapath.Enqueue>): $.datapath.Enqueue {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.datapath.Enqueue): unknown {
  const result: any = {};
  if (value.payload !== undefined) result.payload = tsValueToJsonValueFns.bytes(value.payload);
  if (value.deadlineMs !== undefined) result.deadlineMs = tsValueToJsonValueFns.int64(value.deadlineMs);
  result.kvKeysIfUndelivered = value.kvKeysIfUndelivered.map(value => tsValueToJsonValueFns.bytes(value));
  result.backoffSchedule = value.backoffSchedule.map(value => tsValueToJsonValueFns.uint32(value));
  return result;
}

export function decodeJson(value: any): $.datapath.Enqueue {
  const result = getDefaultValue();
  if (value.payload !== undefined) result.payload = jsonValueToTsValueFns.bytes(value.payload);
  if (value.deadlineMs !== undefined) result.deadlineMs = jsonValueToTsValueFns.int64(value.deadlineMs);
  result.kvKeysIfUndelivered = value.kvKeysIfUndelivered?.map((value: any) => jsonValueToTsValueFns.bytes(value)) ?? [];
  result.backoffSchedule = value.backoffSchedule?.map((value: any) => jsonValueToTsValueFns.uint32(value)) ?? [];
  return result;
}

export function encodeBinary(value: $.datapath.Enqueue): Uint8Array {
  const result: WireMessage = [];
  if (value.payload !== undefined) {
    const tsValue = value.payload;
    result.push(
      [1, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.deadlineMs !== undefined) {
    const tsValue = value.deadlineMs;
    result.push(
      [2, tsValueToWireValueFns.int64(tsValue)],
    );
  }
  for (const tsValue of value.kvKeysIfUndelivered) {
    result.push(
      [3, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  for (const tsValue of value.backoffSchedule) {
    result.push(
      [4, tsValueToWireValueFns.uint32(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.datapath.Enqueue {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  field: {
    const wireValue = wireFields.get(1);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.payload = value;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.int64(wireValue);
    if (value === undefined) break field;
    result.deadlineMs = value;
  }
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 3).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValueToTsValueFns.bytes(wireValue)).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.kvKeysIfUndelivered = value as any;
  }
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 4).map(([, wireValue]) => wireValue);
    const value = Array.from(unpackFns.uint32(wireValues));
    if (!value.length) break collection;
    result.backoffSchedule = value as any;
  }
  return result;
}
