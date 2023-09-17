// @ts-nocheck
import {
  Type as AtomicWriteStatus,
  name2num,
  num2name,
} from "./AtomicWriteStatus.ts";
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
  default as Long,
} from "../../runtime/Long.ts";
import {
  tsValueToWireValueFns,
  wireValueToTsValueFns,
} from "../../runtime/wire/scalar.ts";
import {
  default as deserialize,
} from "../../runtime/wire/deserialize.ts";

export declare namespace $.datapath {
  export type AtomicWriteOutput = {
    status: AtomicWriteStatus;
    versionstamp: Uint8Array;
    primaryIfWriteDisabled: string;
  }
}

export type Type = $.datapath.AtomicWriteOutput;

export function getDefaultValue(): $.datapath.AtomicWriteOutput {
  return {
    status: "AW_UNSPECIFIED",
    versionstamp: new Uint8Array(),
    primaryIfWriteDisabled: "",
  };
}

export function createValue(partialValue: Partial<$.datapath.AtomicWriteOutput>): $.datapath.AtomicWriteOutput {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.datapath.AtomicWriteOutput): unknown {
  const result: any = {};
  if (value.status !== undefined) result.status = tsValueToJsonValueFns.enum(value.status);
  if (value.versionstamp !== undefined) result.versionstamp = tsValueToJsonValueFns.bytes(value.versionstamp);
  if (value.primaryIfWriteDisabled !== undefined) result.primaryIfWriteDisabled = tsValueToJsonValueFns.string(value.primaryIfWriteDisabled);
  return result;
}

export function decodeJson(value: any): $.datapath.AtomicWriteOutput {
  const result = getDefaultValue();
  if (value.status !== undefined) result.status = jsonValueToTsValueFns.enum(value.status) as AtomicWriteStatus;
  if (value.versionstamp !== undefined) result.versionstamp = jsonValueToTsValueFns.bytes(value.versionstamp);
  if (value.primaryIfWriteDisabled !== undefined) result.primaryIfWriteDisabled = jsonValueToTsValueFns.string(value.primaryIfWriteDisabled);
  return result;
}

export function encodeBinary(value: $.datapath.AtomicWriteOutput): Uint8Array {
  const result: WireMessage = [];
  if (value.status !== undefined) {
    const tsValue = value.status;
    result.push(
      [1, { type: WireType.Varint as const, value: new Long(name2num[tsValue as keyof typeof name2num]) }],
    );
  }
  if (value.versionstamp !== undefined) {
    const tsValue = value.versionstamp;
    result.push(
      [2, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.primaryIfWriteDisabled !== undefined) {
    const tsValue = value.primaryIfWriteDisabled;
    result.push(
      [3, tsValueToWireValueFns.string(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.datapath.AtomicWriteOutput {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  field: {
    const wireValue = wireFields.get(1);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.Varint ? num2name[wireValue.value[0] as keyof typeof num2name] : undefined;
    if (value === undefined) break field;
    result.status = value;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.versionstamp = value;
  }
  field: {
    const wireValue = wireFields.get(3);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.string(wireValue);
    if (value === undefined) break field;
    result.primaryIfWriteDisabled = value;
  }
  return result;
}
