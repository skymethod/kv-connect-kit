// @ts-nocheck
import {
  Type as KvValueEncoding,
  name2num,
  num2name,
} from "./KvValueEncoding.ts";
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
  default as Long,
} from "../../runtime/Long.ts";
import {
  default as deserialize,
} from "../../runtime/wire/deserialize.ts";

export declare namespace $.datapath {
  export type KvValue = {
    data: Uint8Array;
    encoding: KvValueEncoding;
  }
}

export type Type = $.datapath.KvValue;

export function getDefaultValue(): $.datapath.KvValue {
  return {
    data: new Uint8Array(),
    encoding: "VE_UNSPECIFIED",
  };
}

export function createValue(partialValue: Partial<$.datapath.KvValue>): $.datapath.KvValue {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.datapath.KvValue): unknown {
  const result: any = {};
  if (value.data !== undefined) result.data = tsValueToJsonValueFns.bytes(value.data);
  if (value.encoding !== undefined) result.encoding = tsValueToJsonValueFns.enum(value.encoding);
  return result;
}

export function decodeJson(value: any): $.datapath.KvValue {
  const result = getDefaultValue();
  if (value.data !== undefined) result.data = jsonValueToTsValueFns.bytes(value.data);
  if (value.encoding !== undefined) result.encoding = jsonValueToTsValueFns.enum(value.encoding) as KvValueEncoding;
  return result;
}

export function encodeBinary(value: $.datapath.KvValue): Uint8Array {
  const result: WireMessage = [];
  if (value.data !== undefined) {
    const tsValue = value.data;
    result.push(
      [1, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.encoding !== undefined) {
    const tsValue = value.encoding;
    result.push(
      [2, { type: WireType.Varint as const, value: new Long(name2num[tsValue as keyof typeof name2num]) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.datapath.KvValue {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  field: {
    const wireValue = wireFields.get(1);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.data = value;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.Varint ? num2name[wireValue.value[0] as keyof typeof num2name] : undefined;
    if (value === undefined) break field;
    result.encoding = value;
  }
  return result;
}
