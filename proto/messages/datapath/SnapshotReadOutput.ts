// @ts-nocheck
import {
  Type as ReadRangeOutput,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./ReadRangeOutput.ts";
import {
  tsValueToJsonValueFns,
  jsonValueToTsValueFns,
} from "../../runtime/json/scalar.ts";
import {
  WireMessage,
  WireType,
} from "../../runtime/wire/index.ts";
import {
  default as serialize,
} from "../../runtime/wire/serialize.ts";
import {
  tsValueToWireValueFns,
  wireValueToTsValueFns,
} from "../../runtime/wire/scalar.ts";
import {
  default as deserialize,
} from "../../runtime/wire/deserialize.ts";

export declare namespace $.datapath {
  export type SnapshotReadOutput = {
    ranges: ReadRangeOutput[];
    readDisabled: boolean;
    regionsIfReadDisabled: string[];
    readIsStronglyConsistent: boolean;
    primaryIfNotStronglyConsistent: string;
  }
}

export type Type = $.datapath.SnapshotReadOutput;

export function getDefaultValue(): $.datapath.SnapshotReadOutput {
  return {
    ranges: [],
    readDisabled: false,
    regionsIfReadDisabled: [],
    readIsStronglyConsistent: false,
    primaryIfNotStronglyConsistent: "",
  };
}

export function createValue(partialValue: Partial<$.datapath.SnapshotReadOutput>): $.datapath.SnapshotReadOutput {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.datapath.SnapshotReadOutput): unknown {
  const result: any = {};
  result.ranges = value.ranges.map(value => encodeJson_1(value));
  if (value.readDisabled !== undefined) result.readDisabled = tsValueToJsonValueFns.bool(value.readDisabled);
  result.regionsIfReadDisabled = value.regionsIfReadDisabled.map(value => tsValueToJsonValueFns.string(value));
  if (value.readIsStronglyConsistent !== undefined) result.readIsStronglyConsistent = tsValueToJsonValueFns.bool(value.readIsStronglyConsistent);
  if (value.primaryIfNotStronglyConsistent !== undefined) result.primaryIfNotStronglyConsistent = tsValueToJsonValueFns.string(value.primaryIfNotStronglyConsistent);
  return result;
}

export function decodeJson(value: any): $.datapath.SnapshotReadOutput {
  const result = getDefaultValue();
  result.ranges = value.ranges?.map((value: any) => decodeJson_1(value)) ?? [];
  if (value.readDisabled !== undefined) result.readDisabled = jsonValueToTsValueFns.bool(value.readDisabled);
  result.regionsIfReadDisabled = value.regionsIfReadDisabled?.map((value: any) => jsonValueToTsValueFns.string(value)) ?? [];
  if (value.readIsStronglyConsistent !== undefined) result.readIsStronglyConsistent = jsonValueToTsValueFns.bool(value.readIsStronglyConsistent);
  if (value.primaryIfNotStronglyConsistent !== undefined) result.primaryIfNotStronglyConsistent = jsonValueToTsValueFns.string(value.primaryIfNotStronglyConsistent);
  return result;
}

export function encodeBinary(value: $.datapath.SnapshotReadOutput): Uint8Array {
  const result: WireMessage = [];
  for (const tsValue of value.ranges) {
    result.push(
      [1, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  if (value.readDisabled !== undefined) {
    const tsValue = value.readDisabled;
    result.push(
      [2, tsValueToWireValueFns.bool(tsValue)],
    );
  }
  for (const tsValue of value.regionsIfReadDisabled) {
    result.push(
      [3, tsValueToWireValueFns.string(tsValue)],
    );
  }
  if (value.readIsStronglyConsistent !== undefined) {
    const tsValue = value.readIsStronglyConsistent;
    result.push(
      [4, tsValueToWireValueFns.bool(tsValue)],
    );
  }
  if (value.primaryIfNotStronglyConsistent !== undefined) {
    const tsValue = value.primaryIfNotStronglyConsistent;
    result.push(
      [5, tsValueToWireValueFns.string(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.datapath.SnapshotReadOutput {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 1).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.ranges = value as any;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bool(wireValue);
    if (value === undefined) break field;
    result.readDisabled = value;
  }
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 3).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValueToTsValueFns.string(wireValue)).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.regionsIfReadDisabled = value as any;
  }
  field: {
    const wireValue = wireFields.get(4);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bool(wireValue);
    if (value === undefined) break field;
    result.readIsStronglyConsistent = value;
  }
  field: {
    const wireValue = wireFields.get(5);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.string(wireValue);
    if (value === undefined) break field;
    result.primaryIfNotStronglyConsistent = value;
  }
  return result;
}
