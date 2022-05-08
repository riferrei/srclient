package srclient

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"time"
)

var _ ISchemaRegistryClient = MockSchemaRegistryClient{}

type MockSchemaRegistryClient struct {
	schemaRegistryURL    string
	credentials          *credentials
	schemaCache          map[string]map[*Schema]int
	idCache              map[int]*Schema
	ids                  *Ids
	codecCreationEnabled bool
}

type Ids struct {
	ids int
}

//Constructor
func CreateMockSchemaRegistryClient(mockURL string) MockSchemaRegistryClient {
	mockClient := MockSchemaRegistryClient{
		schemaRegistryURL:    mockURL,
		credentials:          nil,
		schemaCache:          map[string]map[*Schema]int{},
		idCache:              map[int]*Schema{},
		ids:                  &Ids{ids: 0},
		codecCreationEnabled: false,
	}

	return mockClient
}

/*
Mock Schema creation and registration. CreateSchema behaves in two possible ways according to the scenario:
1. The schema being registered is for an already existing `concrete subject`. In that case,
we increase our schemaID counter and register the schema under that subject in memory.
2. The schema being registered is for a previously unknown `concrete subject`. In that case,
we set this schema as the first version of the subject and store it in memory.

Note that there is no enforcement of schema compatibility, any schema goes for all subjects.
*/
func (mck MockSchemaRegistryClient) CreateSchema(subject string, schema string, schemaType SchemaType, references ...Reference) (*Schema, error) {
	switch schemaType {
	case Avro, Json:
		compiledRegex := regexp.MustCompile(`\r?\n`)
		schema = compiledRegex.ReplaceAllString(schema, " ")
	case Protobuf:
		break
	default:
		return nil, fmt.Errorf("invalid schema type. valid values are Avro, Json, or Protobuf")
	}

	// Subject exists, we just need a new version of the schema registered
	resultFromSchemaCache, ok := mck.schemaCache[subject]
	if ok {
		for s, _ := range resultFromSchemaCache {
			if s.schema == schema {
				registeredID := s.id
				posErr := url.Error{
					Op:  "POST",
					URL: mck.schemaRegistryURL + fmt.Sprintf("/subjects/%s/versions", subject),
					Err: errors.New(fmt.Sprintf("Schema already registered with id %d", registeredID)),
				}
				return nil, &posErr
			}
		}

		mck.ids.ids++
		result := mck.generateVersion(subject, schema, schemaType)
		return result, nil
	} else {

		//Subject does not exist, We need full registration
		mck.ids.ids++
		result := mck.generateVersion(subject, schema, schemaType)
		return result, nil
	}
}

// Returns a Schema for the given ID
func (mck MockSchemaRegistryClient) GetSchema(schemaID int) (*Schema, error) {
	posErr := url.Error{
		Op:  "GET",
		URL: mck.schemaRegistryURL + fmt.Sprintf("/schemas/ids/%d", schemaID),
		Err: errors.New("Schema ID is not registered"),
	}

	thisSchema, ok := mck.idCache[schemaID]
	if !ok {
		return nil, &posErr
	}
	return thisSchema, nil
}

// Returns the highest ordinal version of a Schema for a given `concrete subject`
func (mck MockSchemaRegistryClient) GetLatestSchema(subject string) (*Schema, error) {
	versions, getSchemaVersionErr := mck.GetSchemaVersions(subject)
	if getSchemaVersionErr != nil {
		return nil, getSchemaVersionErr
	}
	if len(versions) == 0 {
		return nil, errors.New("Subject not found")
	}
	latestVersion := versions[len(versions)-1]
	thisSchema, err := mck.GetSchemaByVersion(subject, latestVersion)
	if err != nil {
		return nil, err
	}

	return thisSchema, nil
}

// Returns the array of versions this subject has previously registered
func (mck MockSchemaRegistryClient) GetSchemaVersions(subject string) ([]int, error) {
	versions := mck.allVersions(subject)
	return versions, nil
}

// Returns the given Schema according to the passed in subject and version number
func (mck MockSchemaRegistryClient) GetSchemaByVersion(subject string, version int) (*Schema, error) {
	schema := &Schema{}
	schemaVersionMap, ok := mck.schemaCache[subject]
	if !ok {
		posErr := url.Error{
			Op:  "GET",
			URL: mck.schemaRegistryURL + fmt.Sprintf("/subjects/%s/versions/%d", subject, version),
			Err: errors.New("Subject Not found"),
		}
		return nil, &posErr
	}
	for schemaL, id := range schemaVersionMap {
		if id == version {
			schema = schemaL
		}
	}

	if schema == nil {
		posErr := url.Error{
			Op:  "GET",
			URL: mck.schemaRegistryURL + fmt.Sprintf("/subjects/%s/versions/%d", subject, version),
			Err: errors.New("Version Not found"),
		}
		return nil, &posErr
	}

	return schema, nil
}

// Returns all registered subjects
func (mck MockSchemaRegistryClient) GetSubjects() ([]string, error) {
	allSubjects := make([]string, 0, len(mck.schemaCache))
	for subject := range mck.schemaCache {
		allSubjects = append(allSubjects, subject)
	}
	return allSubjects, nil
}

// GetSubjectsIncludingDeleted returns all registered subjects including those which have been soft deleted
func (mck MockSchemaRegistryClient) GetSubjectsIncludingDeleted() ([]string, error) {
	return nil, errors.New("mock schema registry client can't return soft deleted subjects")
}

// DeleteSubject removes given subject from cache
func (mck MockSchemaRegistryClient) DeleteSubject(subject string, _ bool) error {
	delete(mck.schemaCache, subject)
	return nil
}

func (mck MockSchemaRegistryClient) ChangeSubjectCompatibilityLevel(subject string, compatibility CompatibilityLevel) (*CompatibilityLevel, error) {
	return nil, errors.New("mock schema registry client can't change subject compatibility level")
}

func (mck MockSchemaRegistryClient) GetGlobalCompatibilityLevel() (*CompatibilityLevel, error) {
	return nil, errors.New("mock schema registry client can't return global compatibility level")
}

func (mck MockSchemaRegistryClient) GetCompatibilityLevel(subject string, defaultToGlobal bool) (*CompatibilityLevel, error) {
	return nil, errors.New("mock schema registry client can't return compatibility level")
}

/*
The classes below are implemented to accommodate ISchemaRegistryClient; However, they do nothing.
*/
func (mck MockSchemaRegistryClient) SetCredentials(username string, password string) {
	// Nothing because mockSchemaRegistryClient is actually very vulnerable
}

func (mck MockSchemaRegistryClient) SetTimeout(timeout time.Duration) {
	// Nothing because there is no timeout for cache
}

func (mck MockSchemaRegistryClient) CachingEnabled(value bool) {
	// Nothing because caching is always enabled, duh
}

func (mck MockSchemaRegistryClient) ResetCache() {
	// Nothing because there is no lock for cache
}

func (mck MockSchemaRegistryClient) CodecCreationEnabled(value bool) {
	// Nothing because codecs do not matter in the inMem storage of schemas
}

func (mck MockSchemaRegistryClient) IsSchemaCompatible(subject, schema, version string, schemaType SchemaType) (bool, error) {
	return false, errors.New("mock schema registry client can't check for schema compatibility")
}

func (mck MockSchemaRegistryClient) LookupSchema(subject string, schema string, schemaType SchemaType, references ...Reference) (*Schema, error) {
	return nil, errors.New("mock schema registry client can't lookup schema")
}

/*
These classes are written as helpers and therefore, are not exported.
generateVersion will register a new version of the schema passed, it will NOT do any checks
for the schema being already registered, or for the advancing of the schema ID, these are expected to be
handled beforehand by the environment.
allVersions returns an ordered int[] with all versions for a given subject. It does NOT
qualify for key/value subjects, it expects to have a `concrete subject` passed on to do the checks.
*/
func (mck MockSchemaRegistryClient) generateVersion(subject string, schema string, schemaType SchemaType) *Schema {
	versions := mck.allVersions(subject)
	schemaVersionMap := map[*Schema]int{}
	var currentVersion int
	if len(versions) == 0 {
		currentVersion = 1
	} else {
		schemaVersionMap = mck.schemaCache[subject]
		currentVersion = versions[len(versions)-1] + 1
	}

	// creates a copy
	typeToRegister := schemaType

	schemaToRegister := Schema{
		id:      mck.ids.ids,
		schema:  schema,
		version: currentVersion,
		codec:   nil,
		schemaType: &typeToRegister,
	}

	schemaVersionMap[&schemaToRegister] = currentVersion
	mck.schemaCache[subject] = schemaVersionMap
	mck.idCache[mck.ids.ids] = &schemaToRegister

	return &schemaToRegister
}

func (mck MockSchemaRegistryClient) allVersions(subject string) []int {
	versions := []int{}
	result, ok := mck.schemaCache[subject]
	if ok {
		for _, version := range result {
			versions = append(versions, version)
		}
		sort.Ints(versions)
	}

	return versions
}
