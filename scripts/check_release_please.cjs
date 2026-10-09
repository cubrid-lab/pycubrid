const fs=require('fs'),path=require('path');
// Install outside this Python repo: npm install --prefix /tmp/release-please-check --ignore-scripts release-please@17.6.0
// Run: node scripts/check_release_please.cjs /tmp/release-please-check/node_modules/release-please
// This uses upstream strategy/updaters with a read-only local SCM fixture, no GitHub mutations.
const upstream=path.join(process.argv[2], 'build/src');
if (JSON.parse(fs.readFileSync(path.join(process.argv[2], 'package.json'))).version !== '17.6.0') throw new Error('Use bundled action v5.0.0 core 17.6.0');
require(upstream+'/index.js'); const {Python}=require(upstream+'/strategies/python.js');
const {parseConventionalCommits}=require(upstream+'/commit.js');
const {TagName}=require(upstream+'/util/tag-name.js');
const {Version}=require(upstream+'/version.js');
const root=path.resolve(__dirname,'..');
const config=JSON.parse(fs.readFileSync(path.join(root,'release-please-config.json')));
const {Manifest}=require(upstream+'/manifest.js');
const github={repository:{owner:'cubrid-lab',repo:'pycubrid'},getFileJson:async p=>JSON.parse(fs.readFileSync(path.join(root,p),'utf8')),findFilesByFilenameAndRef:async()=>[],getFileContentsOnBranch:async p=>({parsedContent:fs.readFileSync(path.join(root,p),'utf8')})};
(async()=>{
 const manifest=await Manifest.fromManifest(github,'main');
 const released=JSON.parse(fs.readFileSync(path.join(root,'.release-please-manifest.json')))['.'];
 if(manifest.releasedVersions['.'].toString()!==released || manifest.repositoryConfig['.'].releaseType!=='python') throw new Error('Invalid manifest/config mapping');
 // [message, expected version from the fixed 1.8.0 boundary, expected ### heading (AGENTS.md GitHub Release Policy)]
 const scenarios=[['fix: correct failure','1.8.1','Fixed'],['feat: new optional API','1.9.0','Added'],['feat!: remove old API\n\nBREAKING CHANGE: remove old API','2.0.0','Added'],['docs: improve instructions','1.8.1','Documentation'],['perf: faster fetch','1.8.1','Performance'],['chore: housekeeping',null],['ci: pin action',null],['test: add case',null],['refactor: tidy',null],['fix: explicit override\n\nRelease-As: 1.9.0','1.9.0','Fixed']];
 const allowed=new Set(['Upgrade notes','Added','Changed','Deprecated','Removed','Fixed','Security','Performance','Documentation','CI','Tests','⚠ BREAKING CHANGES']);
 for(const [message,expected,heading] of scenarios){
 const strategy=new Python({...manifest.repositoryConfig['.'],github,targetBranch:'main'});
 const commits=parseConventionalCommits([{sha:'a'.repeat(40),message,files:['pycubrid/__init__.py']}]);
 const candidate=await strategy.buildReleasePullRequest(commits,{tag:new TagName(Version.parse('1.8.0')),sha:'aaaef71deb4f1475f2f6d0dd9d49b146a6d44366',notes:''});
 const actual=candidate?candidate.version.toString():null;
 if(actual!==expected)throw new Error(`${message}: expected ${expected} got ${actual}`);
 const headings=candidate?[...candidate.body.toString().matchAll(/^### (.+)$/gm)].map(m=>m[1]):[];
 if(candidate&&(!headings.includes(heading)||headings.some(h=>!allowed.has(h))))throw new Error(`${message}: expected ### ${heading}, got ${headings}`);
 console.log(JSON.stringify({message,version:actual,headings}));
 if(candidate&&message.startsWith('feat:')){
 const out=process.env.RELEASE_PLEASE_CANDIDATE || '/tmp/pycubrid-release-please-candidate';fs.mkdirSync(out,{recursive:true});
 for(const update of candidate.updates){
 const file=path.join(root,update.path); if(!fs.existsSync(file)&&!update.createIfMissing) continue;
 const content=update.updater.updateContent(fs.existsSync(file)?fs.readFileSync(file,'utf8'):undefined);
 fs.mkdirSync(path.dirname(path.join(out,update.path)),{recursive:true}); fs.writeFileSync(path.join(out,update.path),content);
 console.log('updated '+update.path);
 }
 }
 }
})().catch(err=>{console.error(err);process.exit(1)});
