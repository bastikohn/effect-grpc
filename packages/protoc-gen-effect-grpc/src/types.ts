export interface GeneratorFile {
  readonly protoFileName: string;
  readonly packageName: string;
  readonly importExtension: "js" | "ts";
  readonly imports: ReadonlyArray<ImportModel>;
  readonly enums: ReadonlyArray<EnumModel>;
  readonly messages: ReadonlyArray<MessageModel>;
  readonly services: ReadonlyArray<ServiceModel>;
}

export interface ImportModel {
  readonly protoFileName: string;
  readonly messages: ReadonlyArray<string>;
  readonly enums: ReadonlyArray<string>;
}

export interface EnumModel {
  readonly name: string;
}

export interface MessageModel {
  readonly name: string;
  readonly fields: ReadonlyArray<FieldModel>;
}

export type FieldModel =
  | ScalarFieldModel
  | MessageFieldModel
  | EnumFieldModel
  | WellKnownFieldModel
  | ListFieldModel
  | MapFieldModel
  | OneofFieldModel;

export type FieldValueModel =
  | ScalarFieldModel
  | MessageFieldModel
  | EnumFieldModel
  | WellKnownFieldModel;

export interface ScalarFieldModel {
  readonly kind: "scalar";
  readonly name: string;
  readonly type: ScalarKind;
  readonly unsigned?: boolean;
  readonly optional?: boolean;
}

export interface MessageFieldModel {
  readonly kind: "message";
  readonly name: string;
  readonly messageName: string;
  readonly source: "local" | "imported";
  readonly optional?: boolean;
}

export interface EnumFieldModel {
  readonly kind: "enum";
  readonly name: string;
  readonly enumName: string;
  readonly optional?: boolean;
}

export interface WellKnownFieldModel {
  readonly kind: "well-known";
  readonly name: string;
  readonly type: WellKnownKind;
  readonly optional?: boolean;
}

export interface ListFieldModel {
  readonly kind: "list";
  readonly name: string;
  readonly item: FieldValueModel;
}

export interface MapFieldModel {
  readonly kind: "map";
  readonly name: string;
  readonly key: MapKeyModel;
  readonly value: FieldValueModel;
}

export interface MapKeyModel {
  readonly kind: "map-key";
  readonly type: "number" | "string";
}

export interface OneofFieldModel {
  readonly kind: "oneof";
  readonly name: string;
  readonly converterName: string;
  readonly cases: ReadonlyArray<OneofCaseModel>;
}

export interface OneofCaseModel {
  readonly name: string;
  readonly value: FieldValueModel;
}

export type ScalarKind = "string" | "number" | "boolean" | "bytes" | "bigint";
export type WellKnownKind =
  | "timestamp"
  | "duration"
  | "double-value"
  | "float-value"
  | "int64-value"
  | "uint64-value"
  | "int32-value"
  | "uint32-value"
  | "bool-value"
  | "string-value"
  | "bytes-value"
  | "any"
  | "struct"
  | "value"
  | "list-value"
  | "field-mask";

export interface ServiceModel {
  readonly name: string;
  readonly typeName: string;
  readonly methods: ReadonlyArray<MethodModel>;
}

export interface MethodModel {
  readonly name: string;
  readonly localName: string;
  readonly kind: "unary" | "server-streaming";
  readonly inputType: string;
  readonly outputType: string;
}
