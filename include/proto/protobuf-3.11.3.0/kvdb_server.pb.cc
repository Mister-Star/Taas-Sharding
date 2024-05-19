// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: kvdb_server.proto

#include "kvdb_server.pb.h"

#include <algorithm>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/wire_format_lite.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/wire_format.h>
// @@protoc_insertion_point(includes)
#include <google/protobuf/port_def.inc>
extern PROTOBUF_INTERNAL_EXPORT_kvdb_5fserver_2eproto ::PROTOBUF_NAMESPACE_ID::internal::SCCInfo<0> scc_info_KvDBData_kvdb_5fserver_2eproto;
namespace proto {
class KvDBDataDefaultTypeInternal {
 public:
  ::PROTOBUF_NAMESPACE_ID::internal::ExplicitlyConstructed<KvDBData> _instance;
} _KvDBData_default_instance_;
class KvDBRequestDefaultTypeInternal {
 public:
  ::PROTOBUF_NAMESPACE_ID::internal::ExplicitlyConstructed<KvDBRequest> _instance;
} _KvDBRequest_default_instance_;
class KvDBResponseDefaultTypeInternal {
 public:
  ::PROTOBUF_NAMESPACE_ID::internal::ExplicitlyConstructed<KvDBResponse> _instance;
} _KvDBResponse_default_instance_;
}  // namespace proto
static void InitDefaultsscc_info_KvDBData_kvdb_5fserver_2eproto() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  {
    void* ptr = &::proto::_KvDBData_default_instance_;
    new (ptr) ::proto::KvDBData();
    ::PROTOBUF_NAMESPACE_ID::internal::OnShutdownDestroyMessage(ptr);
  }
  ::proto::KvDBData::InitAsDefaultInstance();
}

::PROTOBUF_NAMESPACE_ID::internal::SCCInfo<0> scc_info_KvDBData_kvdb_5fserver_2eproto =
    {{ATOMIC_VAR_INIT(::PROTOBUF_NAMESPACE_ID::internal::SCCInfoBase::kUninitialized), 0, 0, InitDefaultsscc_info_KvDBData_kvdb_5fserver_2eproto}, {}};

static void InitDefaultsscc_info_KvDBRequest_kvdb_5fserver_2eproto() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  {
    void* ptr = &::proto::_KvDBRequest_default_instance_;
    new (ptr) ::proto::KvDBRequest();
    ::PROTOBUF_NAMESPACE_ID::internal::OnShutdownDestroyMessage(ptr);
  }
  ::proto::KvDBRequest::InitAsDefaultInstance();
}

::PROTOBUF_NAMESPACE_ID::internal::SCCInfo<1> scc_info_KvDBRequest_kvdb_5fserver_2eproto =
    {{ATOMIC_VAR_INIT(::PROTOBUF_NAMESPACE_ID::internal::SCCInfoBase::kUninitialized), 1, 0, InitDefaultsscc_info_KvDBRequest_kvdb_5fserver_2eproto}, {
      &scc_info_KvDBData_kvdb_5fserver_2eproto.base,}};

static void InitDefaultsscc_info_KvDBResponse_kvdb_5fserver_2eproto() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  {
    void* ptr = &::proto::_KvDBResponse_default_instance_;
    new (ptr) ::proto::KvDBResponse();
    ::PROTOBUF_NAMESPACE_ID::internal::OnShutdownDestroyMessage(ptr);
  }
  ::proto::KvDBResponse::InitAsDefaultInstance();
}

::PROTOBUF_NAMESPACE_ID::internal::SCCInfo<1> scc_info_KvDBResponse_kvdb_5fserver_2eproto =
    {{ATOMIC_VAR_INIT(::PROTOBUF_NAMESPACE_ID::internal::SCCInfoBase::kUninitialized), 1, 0, InitDefaultsscc_info_KvDBResponse_kvdb_5fserver_2eproto}, {
      &scc_info_KvDBData_kvdb_5fserver_2eproto.base,}};

static ::PROTOBUF_NAMESPACE_ID::Metadata file_level_metadata_kvdb_5fserver_2eproto[3];
static constexpr ::PROTOBUF_NAMESPACE_ID::EnumDescriptor const** file_level_enum_descriptors_kvdb_5fserver_2eproto = nullptr;
static const ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor* file_level_service_descriptors_kvdb_5fserver_2eproto[2];

const ::PROTOBUF_NAMESPACE_ID::uint32 TableStruct_kvdb_5fserver_2eproto::offsets[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::proto::KvDBData, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  PROTOBUF_FIELD_OFFSET(::proto::KvDBData, op_type_),
  PROTOBUF_FIELD_OFFSET(::proto::KvDBData, key_),
  PROTOBUF_FIELD_OFFSET(::proto::KvDBData, value_),
  PROTOBUF_FIELD_OFFSET(::proto::KvDBData, csn_),
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::proto::KvDBRequest, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  PROTOBUF_FIELD_OFFSET(::proto::KvDBRequest, data_),
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::proto::KvDBResponse, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  PROTOBUF_FIELD_OFFSET(::proto::KvDBResponse, result_),
  PROTOBUF_FIELD_OFFSET(::proto::KvDBResponse, data_),
};
static const ::PROTOBUF_NAMESPACE_ID::internal::MigrationSchema schemas[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  { 0, -1, sizeof(::proto::KvDBData)},
  { 9, -1, sizeof(::proto::KvDBRequest)},
  { 15, -1, sizeof(::proto::KvDBResponse)},
};

static ::PROTOBUF_NAMESPACE_ID::Message const * const file_default_instances[] = {
  reinterpret_cast<const ::PROTOBUF_NAMESPACE_ID::Message*>(&::proto::_KvDBData_default_instance_),
  reinterpret_cast<const ::PROTOBUF_NAMESPACE_ID::Message*>(&::proto::_KvDBRequest_default_instance_),
  reinterpret_cast<const ::PROTOBUF_NAMESPACE_ID::Message*>(&::proto::_KvDBResponse_default_instance_),
};

const char descriptor_table_protodef_kvdb_5fserver_2eproto[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) =
  "\n\021kvdb_server.proto\022\005proto\032\021transaction."
  "proto\"S\n\010KvDBData\022\036\n\007op_type\030\001 \001(\0162\r.pro"
  "to.OpType\022\013\n\003key\030\002 \001(\t\022\r\n\005value\030\003 \001(\t\022\013\n"
  "\003csn\030\004 \001(\004\",\n\013KvDBRequest\022\035\n\004data\030\001 \003(\0132"
  "\017.proto.KvDBData\"=\n\014KvDBResponse\022\016\n\006resu"
  "lt\030\001 \001(\010\022\035\n\004data\030\002 \003(\0132\017.proto.KvDBData2"
  "@\n\016KvDBGetService\022.\n\003Get\022\022.proto.KvDBReq"
  "uest\032\023.proto.KvDBResponse2@\n\016KvDBPutServ"
  "ice\022.\n\003Put\022\022.proto.KvDBRequest\032\023.proto.K"
  "vDBResponseB\021Z\014./taas_proto\200\001\001b\006proto3"
  ;
static const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable*const descriptor_table_kvdb_5fserver_2eproto_deps[1] = {
  &::descriptor_table_transaction_2eproto,
};
static ::PROTOBUF_NAMESPACE_ID::internal::SCCInfoBase*const descriptor_table_kvdb_5fserver_2eproto_sccs[3] = {
  &scc_info_KvDBData_kvdb_5fserver_2eproto.base,
  &scc_info_KvDBRequest_kvdb_5fserver_2eproto.base,
  &scc_info_KvDBResponse_kvdb_5fserver_2eproto.base,
};
static ::PROTOBUF_NAMESPACE_ID::internal::once_flag descriptor_table_kvdb_5fserver_2eproto_once;
static bool descriptor_table_kvdb_5fserver_2eproto_initialized = false;
const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable descriptor_table_kvdb_5fserver_2eproto = {
  &descriptor_table_kvdb_5fserver_2eproto_initialized, descriptor_table_protodef_kvdb_5fserver_2eproto, "kvdb_server.proto", 398,
  &descriptor_table_kvdb_5fserver_2eproto_once, descriptor_table_kvdb_5fserver_2eproto_sccs, descriptor_table_kvdb_5fserver_2eproto_deps, 3, 1,
  schemas, file_default_instances, TableStruct_kvdb_5fserver_2eproto::offsets,
  file_level_metadata_kvdb_5fserver_2eproto, 3, file_level_enum_descriptors_kvdb_5fserver_2eproto, file_level_service_descriptors_kvdb_5fserver_2eproto,
};

// Force running AddDescriptors() at dynamic initialization time.
static bool dynamic_init_dummy_kvdb_5fserver_2eproto = (  ::PROTOBUF_NAMESPACE_ID::internal::AddDescriptors(&descriptor_table_kvdb_5fserver_2eproto), true);
namespace proto {

// ===================================================================

void KvDBData::InitAsDefaultInstance() {
}
class KvDBData::_Internal {
 public:
};

KvDBData::KvDBData()
  : ::PROTOBUF_NAMESPACE_ID::Message(), _internal_metadata_(nullptr) {
  SharedCtor();
  // @@protoc_insertion_point(constructor:proto.KvDBData)
}
KvDBData::KvDBData(const KvDBData& from)
  : ::PROTOBUF_NAMESPACE_ID::Message(),
      _internal_metadata_(nullptr) {
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  key_.UnsafeSetDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
  if (!from._internal_key().empty()) {
    key_.AssignWithDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(), from.key_);
  }
  value_.UnsafeSetDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
  if (!from._internal_value().empty()) {
    value_.AssignWithDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(), from.value_);
  }
  ::memcpy(&csn_, &from.csn_,
    static_cast<size_t>(reinterpret_cast<char*>(&op_type_) -
    reinterpret_cast<char*>(&csn_)) + sizeof(op_type_));
  // @@protoc_insertion_point(copy_constructor:proto.KvDBData)
}

void KvDBData::SharedCtor() {
  ::PROTOBUF_NAMESPACE_ID::internal::InitSCC(&scc_info_KvDBData_kvdb_5fserver_2eproto.base);
  key_.UnsafeSetDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
  value_.UnsafeSetDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
  ::memset(&csn_, 0, static_cast<size_t>(
      reinterpret_cast<char*>(&op_type_) -
      reinterpret_cast<char*>(&csn_)) + sizeof(op_type_));
}

KvDBData::~KvDBData() {
  // @@protoc_insertion_point(destructor:proto.KvDBData)
  SharedDtor();
}

void KvDBData::SharedDtor() {
  key_.DestroyNoArena(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
  value_.DestroyNoArena(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
}

void KvDBData::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}
const KvDBData& KvDBData::default_instance() {
  ::PROTOBUF_NAMESPACE_ID::internal::InitSCC(&::scc_info_KvDBData_kvdb_5fserver_2eproto.base);
  return *internal_default_instance();
}


void KvDBData::Clear() {
// @@protoc_insertion_point(message_clear_start:proto.KvDBData)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  key_.ClearToEmptyNoArena(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
  value_.ClearToEmptyNoArena(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited());
  ::memset(&csn_, 0, static_cast<size_t>(
      reinterpret_cast<char*>(&op_type_) -
      reinterpret_cast<char*>(&csn_)) + sizeof(op_type_));
  _internal_metadata_.Clear();
}

const char* KvDBData::_InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) {
#define CHK_(x) if (PROTOBUF_PREDICT_FALSE(!(x))) goto failure
  while (!ctx->Done(&ptr)) {
    ::PROTOBUF_NAMESPACE_ID::uint32 tag;
    ptr = ::PROTOBUF_NAMESPACE_ID::internal::ReadTag(ptr, &tag);
    CHK_(ptr);
    switch (tag >> 3) {
      // .proto.OpType op_type = 1;
      case 1:
        if (PROTOBUF_PREDICT_TRUE(static_cast<::PROTOBUF_NAMESPACE_ID::uint8>(tag) == 8)) {
          ::PROTOBUF_NAMESPACE_ID::uint64 val = ::PROTOBUF_NAMESPACE_ID::internal::ReadVarint(&ptr);
          CHK_(ptr);
          _internal_set_op_type(static_cast<::proto::OpType>(val));
        } else goto handle_unusual;
        continue;
      // string key = 2;
      case 2:
        if (PROTOBUF_PREDICT_TRUE(static_cast<::PROTOBUF_NAMESPACE_ID::uint8>(tag) == 18)) {
          auto str = _internal_mutable_key();
          ptr = ::PROTOBUF_NAMESPACE_ID::internal::InlineGreedyStringParser(str, ptr, ctx);
          CHK_(::PROTOBUF_NAMESPACE_ID::internal::VerifyUTF8(str, "proto.KvDBData.key"));
          CHK_(ptr);
        } else goto handle_unusual;
        continue;
      // string value = 3;
      case 3:
        if (PROTOBUF_PREDICT_TRUE(static_cast<::PROTOBUF_NAMESPACE_ID::uint8>(tag) == 26)) {
          auto str = _internal_mutable_value();
          ptr = ::PROTOBUF_NAMESPACE_ID::internal::InlineGreedyStringParser(str, ptr, ctx);
          CHK_(::PROTOBUF_NAMESPACE_ID::internal::VerifyUTF8(str, "proto.KvDBData.value"));
          CHK_(ptr);
        } else goto handle_unusual;
        continue;
      // uint64 csn = 4;
      case 4:
        if (PROTOBUF_PREDICT_TRUE(static_cast<::PROTOBUF_NAMESPACE_ID::uint8>(tag) == 32)) {
          csn_ = ::PROTOBUF_NAMESPACE_ID::internal::ReadVarint(&ptr);
          CHK_(ptr);
        } else goto handle_unusual;
        continue;
      default: {
      handle_unusual:
        if ((tag & 7) == 4 || tag == 0) {
          ctx->SetLastTag(tag);
          goto success;
        }
        ptr = UnknownFieldParse(tag, &_internal_metadata_, ptr, ctx);
        CHK_(ptr != nullptr);
        continue;
      }
    }  // switch
  }  // while
success:
  return ptr;
failure:
  ptr = nullptr;
  goto success;
#undef CHK_
}

::PROTOBUF_NAMESPACE_ID::uint8* KvDBData::_InternalSerialize(
    ::PROTOBUF_NAMESPACE_ID::uint8* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:proto.KvDBData)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // .proto.OpType op_type = 1;
  if (this->op_type() != 0) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::WriteEnumToArray(
      1, this->_internal_op_type(), target);
  }

  // string key = 2;
  if (this->key().size() > 0) {
    ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::VerifyUtf8String(
      this->_internal_key().data(), static_cast<int>(this->_internal_key().length()),
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::SERIALIZE,
      "proto.KvDBData.key");
    target = stream->WriteStringMaybeAliased(
        2, this->_internal_key(), target);
  }

  // string value = 3;
  if (this->value().size() > 0) {
    ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::VerifyUtf8String(
      this->_internal_value().data(), static_cast<int>(this->_internal_value().length()),
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::SERIALIZE,
      "proto.KvDBData.value");
    target = stream->WriteStringMaybeAliased(
        3, this->_internal_value(), target);
  }

  // uint64 csn = 4;
  if (this->csn() != 0) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::WriteUInt64ToArray(4, this->_internal_csn(), target);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormat::InternalSerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields(), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:proto.KvDBData)
  return target;
}

size_t KvDBData::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:proto.KvDBData)
  size_t total_size = 0;

  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // string key = 2;
  if (this->key().size() > 0) {
    total_size += 1 +
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::StringSize(
        this->_internal_key());
  }

  // string value = 3;
  if (this->value().size() > 0) {
    total_size += 1 +
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::StringSize(
        this->_internal_value());
  }

  // uint64 csn = 4;
  if (this->csn() != 0) {
    total_size += 1 +
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::UInt64Size(
        this->_internal_csn());
  }

  // .proto.OpType op_type = 1;
  if (this->op_type() != 0) {
    total_size += 1 +
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::EnumSize(this->_internal_op_type());
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    return ::PROTOBUF_NAMESPACE_ID::internal::ComputeUnknownFieldsSize(
        _internal_metadata_, total_size, &_cached_size_);
  }
  int cached_size = ::PROTOBUF_NAMESPACE_ID::internal::ToCachedSize(total_size);
  SetCachedSize(cached_size);
  return total_size;
}

void KvDBData::MergeFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_merge_from_start:proto.KvDBData)
  GOOGLE_DCHECK_NE(&from, this);
  const KvDBData* source =
      ::PROTOBUF_NAMESPACE_ID::DynamicCastToGenerated<KvDBData>(
          &from);
  if (source == nullptr) {
  // @@protoc_insertion_point(generalized_merge_from_cast_fail:proto.KvDBData)
    ::PROTOBUF_NAMESPACE_ID::internal::ReflectionOps::Merge(from, this);
  } else {
  // @@protoc_insertion_point(generalized_merge_from_cast_success:proto.KvDBData)
    MergeFrom(*source);
  }
}

void KvDBData::MergeFrom(const KvDBData& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:proto.KvDBData)
  GOOGLE_DCHECK_NE(&from, this);
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  if (from.key().size() > 0) {

    key_.AssignWithDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(), from.key_);
  }
  if (from.value().size() > 0) {

    value_.AssignWithDefault(&::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(), from.value_);
  }
  if (from.csn() != 0) {
    _internal_set_csn(from._internal_csn());
  }
  if (from.op_type() != 0) {
    _internal_set_op_type(from._internal_op_type());
  }
}

void KvDBData::CopyFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_copy_from_start:proto.KvDBData)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void KvDBData::CopyFrom(const KvDBData& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:proto.KvDBData)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool KvDBData::IsInitialized() const {
  return true;
}

void KvDBData::InternalSwap(KvDBData* other) {
  using std::swap;
  _internal_metadata_.Swap(&other->_internal_metadata_);
  key_.Swap(&other->key_, &::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(),
    GetArenaNoVirtual());
  value_.Swap(&other->value_, &::PROTOBUF_NAMESPACE_ID::internal::GetEmptyStringAlreadyInited(),
    GetArenaNoVirtual());
  swap(csn_, other->csn_);
  swap(op_type_, other->op_type_);
}

::PROTOBUF_NAMESPACE_ID::Metadata KvDBData::GetMetadata() const {
  return GetMetadataStatic();
}


// ===================================================================

void KvDBRequest::InitAsDefaultInstance() {
}
class KvDBRequest::_Internal {
 public:
};

KvDBRequest::KvDBRequest()
  : ::PROTOBUF_NAMESPACE_ID::Message(), _internal_metadata_(nullptr) {
  SharedCtor();
  // @@protoc_insertion_point(constructor:proto.KvDBRequest)
}
KvDBRequest::KvDBRequest(const KvDBRequest& from)
  : ::PROTOBUF_NAMESPACE_ID::Message(),
      _internal_metadata_(nullptr),
      data_(from.data_) {
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  // @@protoc_insertion_point(copy_constructor:proto.KvDBRequest)
}

void KvDBRequest::SharedCtor() {
  ::PROTOBUF_NAMESPACE_ID::internal::InitSCC(&scc_info_KvDBRequest_kvdb_5fserver_2eproto.base);
}

KvDBRequest::~KvDBRequest() {
  // @@protoc_insertion_point(destructor:proto.KvDBRequest)
  SharedDtor();
}

void KvDBRequest::SharedDtor() {
}

void KvDBRequest::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}
const KvDBRequest& KvDBRequest::default_instance() {
  ::PROTOBUF_NAMESPACE_ID::internal::InitSCC(&::scc_info_KvDBRequest_kvdb_5fserver_2eproto.base);
  return *internal_default_instance();
}


void KvDBRequest::Clear() {
// @@protoc_insertion_point(message_clear_start:proto.KvDBRequest)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  data_.Clear();
  _internal_metadata_.Clear();
}

const char* KvDBRequest::_InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) {
#define CHK_(x) if (PROTOBUF_PREDICT_FALSE(!(x))) goto failure
  while (!ctx->Done(&ptr)) {
    ::PROTOBUF_NAMESPACE_ID::uint32 tag;
    ptr = ::PROTOBUF_NAMESPACE_ID::internal::ReadTag(ptr, &tag);
    CHK_(ptr);
    switch (tag >> 3) {
      // repeated .proto.KvDBData data = 1;
      case 1:
        if (PROTOBUF_PREDICT_TRUE(static_cast<::PROTOBUF_NAMESPACE_ID::uint8>(tag) == 10)) {
          ptr -= 1;
          do {
            ptr += 1;
            ptr = ctx->ParseMessage(_internal_add_data(), ptr);
            CHK_(ptr);
            if (!ctx->DataAvailable(ptr)) break;
          } while (::PROTOBUF_NAMESPACE_ID::internal::ExpectTag<10>(ptr));
        } else goto handle_unusual;
        continue;
      default: {
      handle_unusual:
        if ((tag & 7) == 4 || tag == 0) {
          ctx->SetLastTag(tag);
          goto success;
        }
        ptr = UnknownFieldParse(tag, &_internal_metadata_, ptr, ctx);
        CHK_(ptr != nullptr);
        continue;
      }
    }  // switch
  }  // while
success:
  return ptr;
failure:
  ptr = nullptr;
  goto success;
#undef CHK_
}

::PROTOBUF_NAMESPACE_ID::uint8* KvDBRequest::_InternalSerialize(
    ::PROTOBUF_NAMESPACE_ID::uint8* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:proto.KvDBRequest)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // repeated .proto.KvDBData data = 1;
  for (unsigned int i = 0,
      n = static_cast<unsigned int>(this->_internal_data_size()); i < n; i++) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::
      InternalWriteMessage(1, this->_internal_data(i), target, stream);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormat::InternalSerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields(), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:proto.KvDBRequest)
  return target;
}

size_t KvDBRequest::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:proto.KvDBRequest)
  size_t total_size = 0;

  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // repeated .proto.KvDBData data = 1;
  total_size += 1UL * this->_internal_data_size();
  for (const auto& msg : this->data_) {
    total_size +=
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::MessageSize(msg);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    return ::PROTOBUF_NAMESPACE_ID::internal::ComputeUnknownFieldsSize(
        _internal_metadata_, total_size, &_cached_size_);
  }
  int cached_size = ::PROTOBUF_NAMESPACE_ID::internal::ToCachedSize(total_size);
  SetCachedSize(cached_size);
  return total_size;
}

void KvDBRequest::MergeFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_merge_from_start:proto.KvDBRequest)
  GOOGLE_DCHECK_NE(&from, this);
  const KvDBRequest* source =
      ::PROTOBUF_NAMESPACE_ID::DynamicCastToGenerated<KvDBRequest>(
          &from);
  if (source == nullptr) {
  // @@protoc_insertion_point(generalized_merge_from_cast_fail:proto.KvDBRequest)
    ::PROTOBUF_NAMESPACE_ID::internal::ReflectionOps::Merge(from, this);
  } else {
  // @@protoc_insertion_point(generalized_merge_from_cast_success:proto.KvDBRequest)
    MergeFrom(*source);
  }
}

void KvDBRequest::MergeFrom(const KvDBRequest& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:proto.KvDBRequest)
  GOOGLE_DCHECK_NE(&from, this);
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  data_.MergeFrom(from.data_);
}

void KvDBRequest::CopyFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_copy_from_start:proto.KvDBRequest)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void KvDBRequest::CopyFrom(const KvDBRequest& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:proto.KvDBRequest)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool KvDBRequest::IsInitialized() const {
  return true;
}

void KvDBRequest::InternalSwap(KvDBRequest* other) {
  using std::swap;
  _internal_metadata_.Swap(&other->_internal_metadata_);
  data_.InternalSwap(&other->data_);
}

::PROTOBUF_NAMESPACE_ID::Metadata KvDBRequest::GetMetadata() const {
  return GetMetadataStatic();
}


// ===================================================================

void KvDBResponse::InitAsDefaultInstance() {
}
class KvDBResponse::_Internal {
 public:
};

KvDBResponse::KvDBResponse()
  : ::PROTOBUF_NAMESPACE_ID::Message(), _internal_metadata_(nullptr) {
  SharedCtor();
  // @@protoc_insertion_point(constructor:proto.KvDBResponse)
}
KvDBResponse::KvDBResponse(const KvDBResponse& from)
  : ::PROTOBUF_NAMESPACE_ID::Message(),
      _internal_metadata_(nullptr),
      data_(from.data_) {
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  result_ = from.result_;
  // @@protoc_insertion_point(copy_constructor:proto.KvDBResponse)
}

void KvDBResponse::SharedCtor() {
  ::PROTOBUF_NAMESPACE_ID::internal::InitSCC(&scc_info_KvDBResponse_kvdb_5fserver_2eproto.base);
  result_ = false;
}

KvDBResponse::~KvDBResponse() {
  // @@protoc_insertion_point(destructor:proto.KvDBResponse)
  SharedDtor();
}

void KvDBResponse::SharedDtor() {
}

void KvDBResponse::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}
const KvDBResponse& KvDBResponse::default_instance() {
  ::PROTOBUF_NAMESPACE_ID::internal::InitSCC(&::scc_info_KvDBResponse_kvdb_5fserver_2eproto.base);
  return *internal_default_instance();
}


void KvDBResponse::Clear() {
// @@protoc_insertion_point(message_clear_start:proto.KvDBResponse)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  data_.Clear();
  result_ = false;
  _internal_metadata_.Clear();
}

const char* KvDBResponse::_InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) {
#define CHK_(x) if (PROTOBUF_PREDICT_FALSE(!(x))) goto failure
  while (!ctx->Done(&ptr)) {
    ::PROTOBUF_NAMESPACE_ID::uint32 tag;
    ptr = ::PROTOBUF_NAMESPACE_ID::internal::ReadTag(ptr, &tag);
    CHK_(ptr);
    switch (tag >> 3) {
      // bool result = 1;
      case 1:
        if (PROTOBUF_PREDICT_TRUE(static_cast<::PROTOBUF_NAMESPACE_ID::uint8>(tag) == 8)) {
          result_ = ::PROTOBUF_NAMESPACE_ID::internal::ReadVarint(&ptr);
          CHK_(ptr);
        } else goto handle_unusual;
        continue;
      // repeated .proto.KvDBData data = 2;
      case 2:
        if (PROTOBUF_PREDICT_TRUE(static_cast<::PROTOBUF_NAMESPACE_ID::uint8>(tag) == 18)) {
          ptr -= 1;
          do {
            ptr += 1;
            ptr = ctx->ParseMessage(_internal_add_data(), ptr);
            CHK_(ptr);
            if (!ctx->DataAvailable(ptr)) break;
          } while (::PROTOBUF_NAMESPACE_ID::internal::ExpectTag<18>(ptr));
        } else goto handle_unusual;
        continue;
      default: {
      handle_unusual:
        if ((tag & 7) == 4 || tag == 0) {
          ctx->SetLastTag(tag);
          goto success;
        }
        ptr = UnknownFieldParse(tag, &_internal_metadata_, ptr, ctx);
        CHK_(ptr != nullptr);
        continue;
      }
    }  // switch
  }  // while
success:
  return ptr;
failure:
  ptr = nullptr;
  goto success;
#undef CHK_
}

::PROTOBUF_NAMESPACE_ID::uint8* KvDBResponse::_InternalSerialize(
    ::PROTOBUF_NAMESPACE_ID::uint8* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:proto.KvDBResponse)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // bool result = 1;
  if (this->result() != 0) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::WriteBoolToArray(1, this->_internal_result(), target);
  }

  // repeated .proto.KvDBData data = 2;
  for (unsigned int i = 0,
      n = static_cast<unsigned int>(this->_internal_data_size()); i < n; i++) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::
      InternalWriteMessage(2, this->_internal_data(i), target, stream);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormat::InternalSerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields(), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:proto.KvDBResponse)
  return target;
}

size_t KvDBResponse::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:proto.KvDBResponse)
  size_t total_size = 0;

  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // repeated .proto.KvDBData data = 2;
  total_size += 1UL * this->_internal_data_size();
  for (const auto& msg : this->data_) {
    total_size +=
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::MessageSize(msg);
  }

  // bool result = 1;
  if (this->result() != 0) {
    total_size += 1 + 1;
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    return ::PROTOBUF_NAMESPACE_ID::internal::ComputeUnknownFieldsSize(
        _internal_metadata_, total_size, &_cached_size_);
  }
  int cached_size = ::PROTOBUF_NAMESPACE_ID::internal::ToCachedSize(total_size);
  SetCachedSize(cached_size);
  return total_size;
}

void KvDBResponse::MergeFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_merge_from_start:proto.KvDBResponse)
  GOOGLE_DCHECK_NE(&from, this);
  const KvDBResponse* source =
      ::PROTOBUF_NAMESPACE_ID::DynamicCastToGenerated<KvDBResponse>(
          &from);
  if (source == nullptr) {
  // @@protoc_insertion_point(generalized_merge_from_cast_fail:proto.KvDBResponse)
    ::PROTOBUF_NAMESPACE_ID::internal::ReflectionOps::Merge(from, this);
  } else {
  // @@protoc_insertion_point(generalized_merge_from_cast_success:proto.KvDBResponse)
    MergeFrom(*source);
  }
}

void KvDBResponse::MergeFrom(const KvDBResponse& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:proto.KvDBResponse)
  GOOGLE_DCHECK_NE(&from, this);
  _internal_metadata_.MergeFrom(from._internal_metadata_);
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  data_.MergeFrom(from.data_);
  if (from.result() != 0) {
    _internal_set_result(from._internal_result());
  }
}

void KvDBResponse::CopyFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_copy_from_start:proto.KvDBResponse)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void KvDBResponse::CopyFrom(const KvDBResponse& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:proto.KvDBResponse)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool KvDBResponse::IsInitialized() const {
  return true;
}

void KvDBResponse::InternalSwap(KvDBResponse* other) {
  using std::swap;
  _internal_metadata_.Swap(&other->_internal_metadata_);
  data_.InternalSwap(&other->data_);
  swap(result_, other->result_);
}

::PROTOBUF_NAMESPACE_ID::Metadata KvDBResponse::GetMetadata() const {
  return GetMetadataStatic();
}


// ===================================================================

KvDBGetService::~KvDBGetService() {}

const ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor* KvDBGetService::descriptor() {
  ::PROTOBUF_NAMESPACE_ID::internal::AssignDescriptors(&descriptor_table_kvdb_5fserver_2eproto);
  return file_level_service_descriptors_kvdb_5fserver_2eproto[0];
}

const ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor* KvDBGetService::GetDescriptor() {
  return descriptor();
}

void KvDBGetService::Get(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                         const ::proto::KvDBRequest*,
                         ::proto::KvDBResponse*,
                         ::google::protobuf::Closure* done) {
  controller->SetFailed("Method Get() not implemented.");
  done->Run();
}

void KvDBGetService::CallMethod(const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method,
                             ::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                             const ::PROTOBUF_NAMESPACE_ID::Message* request,
                             ::PROTOBUF_NAMESPACE_ID::Message* response,
                             ::google::protobuf::Closure* done) {
  GOOGLE_DCHECK_EQ(method->service(), file_level_service_descriptors_kvdb_5fserver_2eproto[0]);
  switch(method->index()) {
    case 0:
      Get(controller,
             ::PROTOBUF_NAMESPACE_ID::internal::DownCast<const ::proto::KvDBRequest*>(
                 request),
             ::PROTOBUF_NAMESPACE_ID::internal::DownCast<::proto::KvDBResponse*>(
                 response),
             done);
      break;
    default:
      GOOGLE_LOG(FATAL) << "Bad method index; this should never happen.";
      break;
  }
}

const ::PROTOBUF_NAMESPACE_ID::Message& KvDBGetService::GetRequestPrototype(
    const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method) const {
  GOOGLE_DCHECK_EQ(method->service(), descriptor());
  switch(method->index()) {
    case 0:
      return ::proto::KvDBRequest::default_instance();
    default:
      GOOGLE_LOG(FATAL) << "Bad method index; this should never happen.";
      return *::PROTOBUF_NAMESPACE_ID::MessageFactory::generated_factory()
          ->GetPrototype(method->input_type());
  }
}

const ::PROTOBUF_NAMESPACE_ID::Message& KvDBGetService::GetResponsePrototype(
    const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method) const {
  GOOGLE_DCHECK_EQ(method->service(), descriptor());
  switch(method->index()) {
    case 0:
      return ::proto::KvDBResponse::default_instance();
    default:
      GOOGLE_LOG(FATAL) << "Bad method index; this should never happen.";
      return *::PROTOBUF_NAMESPACE_ID::MessageFactory::generated_factory()
          ->GetPrototype(method->output_type());
  }
}

KvDBGetService_Stub::KvDBGetService_Stub(::PROTOBUF_NAMESPACE_ID::RpcChannel* channel)
  : channel_(channel), owns_channel_(false) {}
KvDBGetService_Stub::KvDBGetService_Stub(
    ::PROTOBUF_NAMESPACE_ID::RpcChannel* channel,
    ::PROTOBUF_NAMESPACE_ID::Service::ChannelOwnership ownership)
  : channel_(channel),
    owns_channel_(ownership == ::PROTOBUF_NAMESPACE_ID::Service::STUB_OWNS_CHANNEL) {}
KvDBGetService_Stub::~KvDBGetService_Stub() {
  if (owns_channel_) delete channel_;
}

void KvDBGetService_Stub::Get(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                              const ::proto::KvDBRequest* request,
                              ::proto::KvDBResponse* response,
                              ::google::protobuf::Closure* done) {
  channel_->CallMethod(descriptor()->method(0),
                       controller, request, response, done);
}
// ===================================================================

KvDBPutService::~KvDBPutService() {}

const ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor* KvDBPutService::descriptor() {
  ::PROTOBUF_NAMESPACE_ID::internal::AssignDescriptors(&descriptor_table_kvdb_5fserver_2eproto);
  return file_level_service_descriptors_kvdb_5fserver_2eproto[1];
}

const ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor* KvDBPutService::GetDescriptor() {
  return descriptor();
}

void KvDBPutService::Put(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                         const ::proto::KvDBRequest*,
                         ::proto::KvDBResponse*,
                         ::google::protobuf::Closure* done) {
  controller->SetFailed("Method Put() not implemented.");
  done->Run();
}

void KvDBPutService::CallMethod(const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method,
                             ::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                             const ::PROTOBUF_NAMESPACE_ID::Message* request,
                             ::PROTOBUF_NAMESPACE_ID::Message* response,
                             ::google::protobuf::Closure* done) {
  GOOGLE_DCHECK_EQ(method->service(), file_level_service_descriptors_kvdb_5fserver_2eproto[1]);
  switch(method->index()) {
    case 0:
      Put(controller,
             ::PROTOBUF_NAMESPACE_ID::internal::DownCast<const ::proto::KvDBRequest*>(
                 request),
             ::PROTOBUF_NAMESPACE_ID::internal::DownCast<::proto::KvDBResponse*>(
                 response),
             done);
      break;
    default:
      GOOGLE_LOG(FATAL) << "Bad method index; this should never happen.";
      break;
  }
}

const ::PROTOBUF_NAMESPACE_ID::Message& KvDBPutService::GetRequestPrototype(
    const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method) const {
  GOOGLE_DCHECK_EQ(method->service(), descriptor());
  switch(method->index()) {
    case 0:
      return ::proto::KvDBRequest::default_instance();
    default:
      GOOGLE_LOG(FATAL) << "Bad method index; this should never happen.";
      return *::PROTOBUF_NAMESPACE_ID::MessageFactory::generated_factory()
          ->GetPrototype(method->input_type());
  }
}

const ::PROTOBUF_NAMESPACE_ID::Message& KvDBPutService::GetResponsePrototype(
    const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method) const {
  GOOGLE_DCHECK_EQ(method->service(), descriptor());
  switch(method->index()) {
    case 0:
      return ::proto::KvDBResponse::default_instance();
    default:
      GOOGLE_LOG(FATAL) << "Bad method index; this should never happen.";
      return *::PROTOBUF_NAMESPACE_ID::MessageFactory::generated_factory()
          ->GetPrototype(method->output_type());
  }
}

KvDBPutService_Stub::KvDBPutService_Stub(::PROTOBUF_NAMESPACE_ID::RpcChannel* channel)
  : channel_(channel), owns_channel_(false) {}
KvDBPutService_Stub::KvDBPutService_Stub(
    ::PROTOBUF_NAMESPACE_ID::RpcChannel* channel,
    ::PROTOBUF_NAMESPACE_ID::Service::ChannelOwnership ownership)
  : channel_(channel),
    owns_channel_(ownership == ::PROTOBUF_NAMESPACE_ID::Service::STUB_OWNS_CHANNEL) {}
KvDBPutService_Stub::~KvDBPutService_Stub() {
  if (owns_channel_) delete channel_;
}

void KvDBPutService_Stub::Put(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                              const ::proto::KvDBRequest* request,
                              ::proto::KvDBResponse* response,
                              ::google::protobuf::Closure* done) {
  channel_->CallMethod(descriptor()->method(0),
                       controller, request, response, done);
}

// @@protoc_insertion_point(namespace_scope)
}  // namespace proto
PROTOBUF_NAMESPACE_OPEN
template<> PROTOBUF_NOINLINE ::proto::KvDBData* Arena::CreateMaybeMessage< ::proto::KvDBData >(Arena* arena) {
  return Arena::CreateInternal< ::proto::KvDBData >(arena);
}
template<> PROTOBUF_NOINLINE ::proto::KvDBRequest* Arena::CreateMaybeMessage< ::proto::KvDBRequest >(Arena* arena) {
  return Arena::CreateInternal< ::proto::KvDBRequest >(arena);
}
template<> PROTOBUF_NOINLINE ::proto::KvDBResponse* Arena::CreateMaybeMessage< ::proto::KvDBResponse >(Arena* arena) {
  return Arena::CreateInternal< ::proto::KvDBResponse >(arena);
}
PROTOBUF_NAMESPACE_CLOSE

// @@protoc_insertion_point(global_scope)
#include <google/protobuf/port_undef.inc>